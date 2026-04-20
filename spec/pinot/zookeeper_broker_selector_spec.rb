require "spec_helper"
require "pinot/zookeeper_broker_selector"

RSpec.describe Pinot::ZookeeperBrokerSelector do
  # Minimal fake ZK client — no real ZooKeeper needed.
  subject(:selector) do
    described_class.new(zk_path: "localhost:2181", zk_client: fake_zk)
  end

  let(:external_view_json) do
    JSON.dump(
      "mapFields" => {
        "baseballStats_OFFLINE" => {
          "Broker_host1_8000" => "ONLINE",
          "Broker_host2_8001" => "ONLINE",
          "Broker_host3_8002" => "OFFLINE"
        },
        "pizzaOrders_REALTIME" => {
          "Broker_host1_8000" => "ONLINE"
        }
      }
    )
  end

  # A FakeZK double that supports the three methods the selector calls.
  let(:fake_zk) do
    registered_blocks = {}
    double("FakeZK").tap do |zk|
      allow(zk).to receive(:get) do |path|
        expect(path).to eq(Pinot::ZookeeperBrokerSelector::BROKER_EXTERNAL_VIEW_PATH)
        [external_view_json, nil]
      end

      allow(zk).to receive(:register) do |path, &block|
        registered_blocks[path] = block
      end

      # Expose stored blocks so tests can fire them
      allow(zk).to receive_messages(exists?: true, _registered_blocks: registered_blocks)
    end
  end

  describe "#init" do
    it "populates the all_broker_list" do
      selector.init
      brokers = selector.instance_variable_get(:@all_broker_list)
      expect(brokers).to include("host1:8000", "host2:8001")
    end

    it "populates the table_broker_map with suffix-stripped keys" do
      selector.init
      map = selector.instance_variable_get(:@table_broker_map)
      # _OFFLINE / _REALTIME suffixes are stripped, matching Go's extractTableName behaviour
      expect(map.keys).to include("baseballStats", "pizzaOrders")
    end
  end

  describe "#parse_external_view (via init)" do
    before { selector.init }

    it "converts Broker_host_port keys to host:port" do
      map = selector.instance_variable_get(:@table_broker_map)
      expect(map["baseballStats"]).to include("host1:8000", "host2:8001")
    end

    it "excludes brokers that are not ONLINE" do
      map = selector.instance_variable_get(:@table_broker_map)
      # host3:8002 is OFFLINE — must not appear
      expect(map["baseballStats"]).not_to include("host3:8002")
    end
  end

  describe "#select_broker" do
    before { selector.init }

    it "returns a broker from the all-broker list when no table is given" do
      broker = selector.select_broker("")
      expect(broker).to(satisfy { |b| ["host1:8000", "host2:8001"].include?(b) })
    end

    it "returns a broker for a specific table (strips suffix before lookup)" do
      # select_broker calls extract_table_name which strips _REALTIME;
      # the map is also keyed without suffix, so the lookup succeeds.
      broker = selector.select_broker("pizzaOrders_REALTIME")
      expect(broker).to eq("host1:8000")
    end

    it "raises TableNotFoundError for an unknown table" do
      expect { selector.select_broker("unknownTable") }
        .to raise_error(Pinot::TableNotFoundError, /unknownTable/)
    end
  end

  describe "ZK watcher callback" do
    it "re-fetches broker data when the watcher fires" do
      selector.init

      updated_json = JSON.dump(
        "mapFields" => {
          "baseballStats_OFFLINE" => {
            "Broker_host9_9999" => "ONLINE"
          }
        }
      )
      # After update, host9:9999 should be in the broker list

      # Swap out what get() returns for subsequent calls
      allow(fake_zk).to receive(:get).and_return([updated_json, nil])

      # Fire the stored watcher block
      block = fake_zk._registered_blocks[Pinot::ZookeeperBrokerSelector::BROKER_EXTERNAL_VIEW_PATH]
      expect(block).not_to be_nil
      block.call(nil)

      brokers = selector.instance_variable_get(:@all_broker_list)
      expect(brokers).to include("host9:9999")
    end
  end

  describe "missing zk gem" do
    it "raises ConfigurationError with a helpful message" do
      # Hide the ZK constant if it exists, then test build_zk_client
      selector_no_client = described_class.new(zk_path: "localhost:2181")

      # Simulate LoadError when `require "zk"` is called
      allow(selector_no_client).to receive(:require).with("zk").and_raise(LoadError)

      expect { selector_no_client.send(:build_zk_client) }
        .to raise_error(Pinot::ConfigurationError, /zk.*gem.*required/i)
    end

    it "calls ZK.new when zk gem is available and no client injected" do
      fake_zk = double("ZK real")
      stub_const("ZK", Class.new { def self.new(_path); end })
      allow(ZK).to receive(:new).with("localhost:2181").and_return(fake_zk)

      selector_no_client = described_class.new(zk_path: "localhost:2181")
      # Stub require so the gem "load" step succeeds without the real gem
      allow(selector_no_client).to receive(:require).with("zk")
      result = selector_no_client.send(:build_zk_client)
      expect(result).to eq fake_zk
    end
  end

  describe "parse_external_view edge cases" do
    it "skips broker key with only 1 part (no underscore separation)" do
      json = JSON.dump(
        "mapFields" => {
          "myTable_OFFLINE" => {
            "Broker" => "ONLINE"
          }
        }
      )
      zk = double("ZK")
      allow(zk).to receive(:register)
      allow(zk).to receive_messages(get: [json, nil], exists?: true)

      sel = described_class.new(zk_path: "localhost:2181", zk_client: zk)
      sel.init

      map = sel.instance_variable_get(:@table_broker_map)
      expect(map["myTable"]).to eq([])
    end

    it "skips broker key where last segment is non-numeric" do
      json = JSON.dump(
        "mapFields" => {
          "myTable_OFFLINE" => {
            "Broker_host1_notaport" => "ONLINE"
          }
        }
      )
      zk = double("ZK")
      allow(zk).to receive(:register)
      allow(zk).to receive_messages(get: [json, nil], exists?: true)

      sel = described_class.new(zk_path: "localhost:2181", zk_client: zk)
      sel.init

      map = sel.instance_variable_get(:@table_broker_map)
      expect(map["myTable"]).to eq([])
    end

    it "empty mapFields results in empty broker list" do
      json = JSON.dump("mapFields" => {})
      zk = double("ZK")
      allow(zk).to receive(:register)
      allow(zk).to receive_messages(get: [json, nil], exists?: true)

      sel = described_class.new(zk_path: "localhost:2181", zk_client: zk)
      sel.init

      all_brokers = sel.instance_variable_get(:@all_broker_list)
      expect(all_brokers).to eq([])
    end

    it "select_broker raises BrokerNotFoundError when empty mapFields and table name empty" do
      json = JSON.dump("mapFields" => {})
      zk = double("ZK")
      allow(zk).to receive(:register)
      allow(zk).to receive_messages(get: [json, nil], exists?: true)

      sel = described_class.new(zk_path: "localhost:2181", zk_client: zk)
      sel.init

      expect { sel.select_broker("") }
        .to raise_error(Pinot::BrokerNotFoundError, /no available broker/)
    end

    it "missing mapFields key is treated as empty (no crash)" do
      json = JSON.dump({})
      zk = double("ZK")
      allow(zk).to receive(:register)
      allow(zk).to receive_messages(get: [json, nil], exists?: true)

      sel = described_class.new(zk_path: "localhost:2181", zk_client: zk)
      expect { sel.init }.not_to raise_error
    end
  end
end
