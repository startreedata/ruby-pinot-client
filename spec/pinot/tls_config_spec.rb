RSpec.describe Pinot::TlsConfig do
  describe "default values" do
    subject { described_class.new }

    it "has nil ca_cert_file by default" do
      expect(subject.ca_cert_file).to be_nil
    end

    it "has nil client_cert_file by default" do
      expect(subject.client_cert_file).to be_nil
    end

    it "has nil client_key_file by default" do
      expect(subject.client_key_file).to be_nil
    end

    it "has insecure_skip_verify false by default" do
      expect(subject.insecure_skip_verify).to be false
    end
  end

  describe "constructor kwargs" do
    it "can set all fields via constructor" do
      tls = described_class.new(
        ca_cert_file: "/path/to/ca.pem",
        client_cert_file: "/path/to/client.crt",
        client_key_file: "/path/to/client.key",
        insecure_skip_verify: true
      )
      expect(tls.ca_cert_file).to eq "/path/to/ca.pem"
      expect(tls.client_cert_file).to eq "/path/to/client.crt"
      expect(tls.client_key_file).to eq "/path/to/client.key"
      expect(tls.insecure_skip_verify).to be true
    end
  end

  describe "mutation after construction" do
    it "can mutate ca_cert_file after construction" do
      tls = described_class.new
      tls.ca_cert_file = "/new/ca.pem"
      expect(tls.ca_cert_file).to eq "/new/ca.pem"
    end

    it "can mutate client_cert_file after construction" do
      tls = described_class.new
      tls.client_cert_file = "/new/client.crt"
      expect(tls.client_cert_file).to eq "/new/client.crt"
    end

    it "can mutate client_key_file after construction" do
      tls = described_class.new
      tls.client_key_file = "/new/client.key"
      expect(tls.client_key_file).to eq "/new/client.key"
    end

    it "can mutate insecure_skip_verify after construction" do
      tls = described_class.new
      tls.insecure_skip_verify = true
      expect(tls.insecure_skip_verify).to be true
    end
  end
end
