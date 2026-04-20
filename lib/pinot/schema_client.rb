require "net/http"
require "uri"
require "json"

module Pinot
  # Thin client for the Pinot Controller REST API — table listing and schema
  # introspection. Useful for tooling, migrations, and debugging column types.
  #
  # == Usage
  #
  #   client = Pinot::SchemaClient.new("http://controller:9000")
  #
  #   client.list_tables                      # => ["baseballStats", "orders", ...]
  #   client.get_schema("baseballStats")      # => Hash (raw schema JSON)
  #   client.get_table_config("baseballStats")# => Hash (raw tableConfig JSON)
  #   client.table_exists?("orders")          # => true / false
  #
  # == Authentication / extra headers
  #
  #   client = Pinot::SchemaClient.new(
  #     "https://controller:9000",
  #     headers: { "Authorization" => "Bearer <token>" }
  #   )
  #
  # The client is intentionally stateless and lightweight — it uses a shared
  # HttpClient (connection pool) but does not perform background polling.
  class SchemaClient
    TABLES_PATH      = "/tables".freeze
    SCHEMA_PATH      = "/schemas/%<table>s".freeze
    TABLE_CONFIG_PATH = "/tables/%<table>s".freeze

    # @param controller_address [String] base URL e.g. "controller:9000" or
    #   "http://controller:9000" or "https://controller:9000"
    # @param headers [Hash] extra HTTP headers for every request
    # @param http_client [HttpClient, nil] optional pre-configured client
    def initialize(controller_address, headers: {}, http_client: nil)
      @base    = normalize_address(controller_address)
      @headers = { "Accept" => "application/json" }.merge(headers)
      @http    = http_client || HttpClient.new
    end

    # Returns an array of all table names known to the controller.
    #
    # @return [Array<String>]
    def list_tables
      body = get_json(TABLES_PATH)
      body["tables"] || []
    end

    # Returns the schema for a table as a Hash.
    #
    # @param table [String] table name (without _OFFLINE / _REALTIME suffix)
    # @return [Hash] raw schema JSON
    # @raise [TableNotFoundError] if the table or schema does not exist (404)
    # @raise [TransportError] on other non-200 responses
    def get_schema(table)
      get_json(format(SCHEMA_PATH, table: table))
    end

    # Returns the full table config (including segmentsConfig, tableIndexConfig,
    # tenants, etc.) for a table as a Hash.
    #
    # @param table [String] table name (without _OFFLINE / _REALTIME suffix)
    # @return [Hash] raw tableConfig JSON
    # @raise [TableNotFoundError] if the table does not exist (404)
    # @raise [TransportError] on other non-200 responses
    def get_table_config(table)
      get_json(format(TABLE_CONFIG_PATH, table: table))
    end

    # Returns true when the controller knows about the given table.
    #
    # @param table [String]
    # @return [Boolean]
    def table_exists?(table)
      get_table_config(table)
      true
    rescue TableNotFoundError
      false
    end

    # Returns the column names and their data types for a table.
    #
    # Convenience wrapper around get_schema that returns a flat Hash:
    #   { "playerId" => "INT", "playerName" => "STRING", ... }
    #
    # @param table [String]
    # @return [Hash{String => String}]
    def column_types(table)
      schema = get_schema(table)
      dims   = schema["dimensionFieldSpecs"] || []
      metrics = schema["metricFieldSpecs"] || []
      date_time = schema["dateTimeFieldSpecs"] || []
      (dims + metrics + date_time).to_h do |spec|
        [spec["name"], spec["dataType"]]
      end
    end

    private

    def normalize_address(address)
      addr = address.to_s.chomp("/")
      addr.start_with?("http://", "https://") ? addr : "http://#{addr}"
    end

    def get_json(path)
      url  = "#{@base}#{path}"
      resp = @http.get(url, headers: @headers)

      case resp.code.to_i
      when 200
        JSON.parse(resp.body)
      when 404
        raise TableNotFoundError, "not found: #{path}"
      else
        raise TransportError, "controller returned HTTP #{resp.code} for #{path}"
      end
    rescue JSON::ParserError => e
      raise TransportError, "invalid JSON from controller: #{e.message}"
    end
  end
end
