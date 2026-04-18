RSpec.describe "Pinot error classes" do
  it "defines Pinot::Error" do
    expect(defined?(Pinot::Error)).to be_truthy
  end

  it "defines Pinot::BrokerNotFoundError" do
    expect(defined?(Pinot::BrokerNotFoundError)).to be_truthy
  end

  it "defines Pinot::TableNotFoundError" do
    expect(defined?(Pinot::TableNotFoundError)).to be_truthy
  end

  it "defines Pinot::TransportError" do
    expect(defined?(Pinot::TransportError)).to be_truthy
  end

  it "defines Pinot::PreparedStatementClosedError" do
    expect(defined?(Pinot::PreparedStatementClosedError)).to be_truthy
  end

  it "defines Pinot::ConfigurationError" do
    expect(defined?(Pinot::ConfigurationError)).to be_truthy
  end

  it "Pinot::Error inherits from StandardError" do
    expect(Pinot::Error.ancestors).to include(StandardError)
  end

  it "Pinot::BrokerNotFoundError inherits from Pinot::Error" do
    expect(Pinot::BrokerNotFoundError.ancestors).to include(Pinot::Error)
  end

  it "Pinot::TableNotFoundError inherits from Pinot::Error" do
    expect(Pinot::TableNotFoundError.ancestors).to include(Pinot::Error)
  end

  it "Pinot::TransportError inherits from Pinot::Error" do
    expect(Pinot::TransportError.ancestors).to include(Pinot::Error)
  end

  it "Pinot::PreparedStatementClosedError inherits from Pinot::Error" do
    expect(Pinot::PreparedStatementClosedError.ancestors).to include(Pinot::Error)
  end

  it "Pinot::ConfigurationError inherits from Pinot::Error" do
    expect(Pinot::ConfigurationError.ancestors).to include(Pinot::Error)
  end

  it "can rescue BrokerNotFoundError as Pinot::Error" do
    expect do
      begin
        raise Pinot::BrokerNotFoundError, "no broker"
      rescue Pinot::Error
        # rescued successfully
      end
    end.not_to raise_error
  end

  it "can rescue TableNotFoundError as Pinot::Error" do
    expect do
      begin
        raise Pinot::TableNotFoundError, "no table"
      rescue Pinot::Error
        # rescued successfully
      end
    end.not_to raise_error
  end
end
