RSpec.describe Pinot::Request do
  describe "struct members" do
    it "has the correct members list" do
      expect(described_class.members).to eq(
        %i[query_format query trace use_multistage_engine query_timeout_ms]
      )
    end
  end

  describe "default values" do
    subject(:req) { described_class.new }

    it "query_format defaults to nil" do
      expect(req.query_format).to be_nil
    end

    it "query defaults to nil" do
      expect(req.query).to be_nil
    end

    it "trace defaults to nil" do
      expect(req.trace).to be_nil
    end

    it "use_multistage_engine defaults to nil" do
      expect(req.use_multistage_engine).to be_nil
    end

    it "query_timeout_ms defaults to nil" do
      expect(req.query_timeout_ms).to be_nil
    end
  end

  describe "positional construction" do
    subject(:req) { described_class.new("sql", "SELECT 1", false, false, 5000) }

    it "sets query_format" do
      expect(req.query_format).to eq("sql")
    end

    it "sets query" do
      expect(req.query).to eq("SELECT 1")
    end

    it "sets trace" do
      expect(req.trace).to be(false)
    end

    it "sets use_multistage_engine" do
      expect(req.use_multistage_engine).to be(false)
    end

    it "sets query_timeout_ms" do
      expect(req.query_timeout_ms).to eq(5000)
    end
  end

  describe "keyword-like struct access" do
    it "allows reading and writing all fields by accessor name" do
      req = described_class.new
      req.query_format = "sql"
      req.query = "SELECT * FROM t"
      req.trace = true
      req.use_multistage_engine = true
      req.query_timeout_ms = 10_000

      expect(req.query_format).to eq("sql")
      expect(req.query).to eq("SELECT * FROM t")
      expect(req.trace).to be(true)
      expect(req.use_multistage_engine).to be(true)
      expect(req.query_timeout_ms).to eq(10_000)
    end
  end
end
