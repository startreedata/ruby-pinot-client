module Pinot
  class TlsConfig
    attr_accessor :ca_cert_file,        # path to CA cert PEM file (String, optional)
                  :client_cert_file,    # path to client cert PEM file (String, optional)
                  :client_key_file,     # path to client key PEM file (String, optional)
                  :insecure_skip_verify # boolean, skip server cert verification (default: false)

    def initialize(ca_cert_file: nil, client_cert_file: nil, client_key_file: nil, insecure_skip_verify: false)
      @ca_cert_file = ca_cert_file
      @client_cert_file = client_cert_file
      @client_key_file = client_key_file
      @insecure_skip_verify = insecure_skip_verify
    end
  end
end
