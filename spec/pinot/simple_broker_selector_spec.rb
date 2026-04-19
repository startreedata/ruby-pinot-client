RSpec.describe Pinot::SimpleBrokerSelector do
  describe "#init" do
    it "succeeds with a populated list" do
      sel = Pinot::SimpleBrokerSelector.new(["broker1:8000", "broker2:8000"])
      expect { sel.init }.not_to raise_error
    end

    it "raises with an empty list" do
      sel = Pinot::SimpleBrokerSelector.new([])
      expect { sel.init }.to raise_error(Pinot::BrokerNotFoundError, "no pre-configured broker lists")
    end
  end

  describe "#select_broker" do
    it "returns a broker from the list" do
      list = ["broker1:8000", "broker2:8000"]
      sel = Pinot::SimpleBrokerSelector.new(list)
      10.times do
        expect(list).to include(sel.select_broker(""))
      end
    end

    it "raises when list is empty" do
      sel = Pinot::SimpleBrokerSelector.new([])
      expect { sel.select_broker("") }.to raise_error(Pinot::BrokerNotFoundError, "no pre-configured broker lists")
    end

    it "cycles through all brokers in order (round-robin)" do
      list = ["broker1:8000", "broker2:8000", "broker3:8000"]
      sel = Pinot::SimpleBrokerSelector.new(list)
      results = (list.size * 2).times.map { sel.select_broker("") }
      expect(results).to eq(list * 2)
    end
  end
end
