RSpec.describe Pinot::TableAwareBrokerSelector do
  describe "#init" do
    it "raises NotImplementedError because subclasses must implement it" do
      expect { Pinot::TableAwareBrokerSelector.new.init }
        .to raise_error(NotImplementedError, /subclasses must implement init/)
    end
  end

  let(:selector) do
    sel = Pinot::TableAwareBrokerSelector.new
    sel.update_broker_data(["host1:8000", "host2:8000"], {
      "myTable" => ["host1:8000"],
      "emptyTable" => []
    })
    sel
  end

  describe "#select_broker" do
    it "strips _OFFLINE suffix and selects broker" do
      broker = selector.select_broker("myTable_OFFLINE")
      expect(broker).to eq "host1:8000"
    end

    it "strips _REALTIME suffix and selects broker" do
      broker = selector.select_broker("myTable_REALTIME")
      expect(broker).to eq "host1:8000"
    end

    it "selects from all brokers for empty table name" do
      broker = selector.select_broker("")
      expect(["host1:8000", "host2:8000"]).to include(broker)
    end

    it "selects the mapped broker for a named table" do
      expect(selector.select_broker("myTable")).to eq "host1:8000"
    end

    it "raises for unknown table" do
      expect { selector.select_broker("unknownTable") }.to raise_error(Pinot::TableNotFoundError, /unable to find table: unknownTable/)
    end

    it "raises for table with empty broker list" do
      expect { selector.select_broker("emptyTable") }.to raise_error(Pinot::BrokerNotFoundError, /no available broker for table: emptyTable/)
    end

    it "raises when all_broker_list is empty and table is empty" do
      sel = Pinot::TableAwareBrokerSelector.new
      expect { sel.select_broker("") }.to raise_error(Pinot::BrokerNotFoundError, /no available broker/)
    end
  end

  describe "thread safety" do
    it "handles concurrent writes without deadlock" do
      sel = Pinot::TableAwareBrokerSelector.new

      threads = 10.times.map do |i|
        Thread.new do
          sel.update_broker_data(["h#{i}:8000"], { "t#{i}" => ["h#{i}:8000"] })
        end
      end
      threads.each(&:join)

      # Should have some data set
      expect(sel.instance_variable_get(:@all_broker_list)).not_to be_nil
    end
  end

  describe "suffix stripping" do
    it "strips _REALTIME suffix so the bare table name is used for lookup" do
      # Table stored without suffix, queried with _REALTIME
      sel = Pinot::TableAwareBrokerSelector.new
      sel.update_broker_data(["host1:8000"], { "myTable" => ["host1:8000"] })
      expect(sel.select_broker("myTable_REALTIME")).to eq "host1:8000"
    end

    it "strips _OFFLINE suffix so the bare table name is used for lookup" do
      sel = Pinot::TableAwareBrokerSelector.new
      sel.update_broker_data(["host1:8000"], { "myTable" => ["host1:8000"] })
      expect(sel.select_broker("myTable_OFFLINE")).to eq "host1:8000"
    end

    it "passes through a table name with no recognised suffix unchanged" do
      sel = Pinot::TableAwareBrokerSelector.new
      sel.update_broker_data(["host1:8000"], { "myTable_HYBRID" => ["host1:8000"] })
      expect(sel.select_broker("myTable_HYBRID")).to eq "host1:8000"
    end
  end

  describe "empty broker list for known table" do
    it "raises BrokerNotFoundError when broker list for the table is empty" do
      sel = Pinot::TableAwareBrokerSelector.new
      sel.update_broker_data(["host1:8000"], { "emptyTable" => [] })
      expect { sel.select_broker("emptyTable") }
        .to raise_error(Pinot::BrokerNotFoundError, /no available broker for table: emptyTable/)
    end
  end

  describe "#update_broker_data atomicity" do
    it "replaces old broker data atomically" do
      sel = Pinot::TableAwareBrokerSelector.new
      sel.update_broker_data(["old:8000"], { "t1" => ["old:8000"] })

      expect(sel.select_broker("t1")).to eq "old:8000"

      sel.update_broker_data(["new:9000"], { "t2" => ["new:9000"] })

      # old table is gone
      expect { sel.select_broker("t1") }.to raise_error(Pinot::TableNotFoundError)
      # new table is present
      expect(sel.select_broker("t2")).to eq "new:9000"
    end
  end
end
