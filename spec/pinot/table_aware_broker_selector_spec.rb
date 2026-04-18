RSpec.describe Pinot::TableAwareBrokerSelector do
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
end
