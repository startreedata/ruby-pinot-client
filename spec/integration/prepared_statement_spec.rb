require_relative "integration_helper"

RSpec.describe "Pinot integration — PreparedStatement" do
  let(:table) { "baseballStats" }

  def clients
    [pinot_client, pinot_client]
  end

  describe "basic prepared statement" do
    it "executes with SetString and returns results" do
      clients.each do |client|
        stmt = client.prepare(table, "select count(*) as cnt from baseballStats where teamID = ? limit 1")
        expect(stmt.get_parameter_count).to eq 1
        expect(stmt.get_query).to include("teamID = ?")

        stmt.set_string(1, "SFN")
        resp = stmt.execute
        expect(resp).not_to be_nil
        expect(resp.result_table.row_count).to eq 1
        expect(resp.result_table.column_name(0)).to eq "cnt"
        expect(resp.result_table.get_long(0, 0)).to be > 0

        stmt.close
      end
    end
  end

  describe "multiple parameters" do
    it "queries with team, year, and limit" do
      clients.each do |client|
        stmt = client.prepare(
          table,
          "select playerName, sum(homeRuns) as totalHomeRuns from baseballStats " \
          "where teamID = ? and yearID >= ? group by playerID, playerName " \
          "order by totalHomeRuns desc limit ?"
        )
        expect(stmt.get_parameter_count).to eq 3

        stmt.set_string(1, "NYA")
        stmt.set_int(2, 2000)
        stmt.set_int(3, 5)

        resp = stmt.execute
        expect(resp).not_to be_nil
        expect(resp.result_table.row_count).to be <= 5
        expect(resp.result_table.column_count).to eq 2
        expect(resp.result_table.column_name(0)).to eq "playerName"
        expect(resp.result_table.column_name(1)).to eq "totalHomeRuns"

        stmt.close
      end
    end
  end

  describe "statement reuse across teams" do
    it "returns results for multiple teams" do
      client = pinot_client
      stmt = client.prepare(
        table,
        "select count(*) as playerCount, sum(homeRuns) as totalHomeRuns from baseballStats where teamID = ?"
      )

      %w[NYA BOS LAA].each do |team|
        stmt.clear_parameters
        stmt.set_string(1, team)

        resp = stmt.execute
        expect(resp.result_table.row_count).to eq 1
        expect(resp.result_table.get_long(0, 0)).to be > 0
      end

      stmt.close
    end
  end

  describe "execute_with_params" do
    it "executes with inline params" do
      client = pinot_client
      stmt = client.prepare(
        table,
        "select count(*) as cnt from baseballStats where yearID between ? and ? and homeRuns >= ?"
      )

      resp = stmt.execute_with_params(2000, 2010, 20)
      expect(resp.result_table.get_long(0, 0)).to be >= 0

      resp2 = stmt.execute_with_params(1990, 1999, 30)
      expect(resp2.result_table.get_long(0, 0)).to be >= 0

      stmt.close
    end
  end

  describe "different parameter types" do
    it "works with set_int and set_int64" do
      client = pinot_client
      stmt = client.prepare(
        table,
        "select count(*) as cnt from baseballStats where yearID = ? and homeRuns >= ?"
      )

      stmt.set_int(1, 2001)
      stmt.set_int(2, 25)
      resp = stmt.execute
      expect(resp.result_table.get_long(0, 0)).to be >= 0

      stmt.set(1, 2005)
      stmt.set(2, 30)
      resp2 = stmt.execute
      expect(resp2.result_table.get_long(0, 0)).to be >= 0

      stmt.close
    end
  end

  describe "multistage engine" do
    it "executes with multistage engine enabled" do
      client = pinot_client(use_multistage: true)
      stmt = client.prepare(
        table,
        "select teamID, count(*) as cnt from baseballStats where yearID = ? group by teamID order by cnt desc limit ?"
      )

      resp = stmt.execute_with_params(2000, 10)
      expect(resp).not_to be_nil
      expect(resp.result_table.row_count).to be <= 10
      expect(resp.result_table.column_count).to eq 2

      stmt.close
    end
  end
end
