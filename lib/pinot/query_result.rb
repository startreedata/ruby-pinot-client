module Pinot
  QueryResult = Struct.new(:table, :query, :response, :error, keyword_init: true) do
    def success?
      error.nil?
    end

    def error?
      !error.nil?
    end
  end
end
