require 'openssl'

module Moped
  module Sockets

    # This is a wrapper around a tcp socket.
    class SSL < OpenSSL::SSL::SSLSocket
      include Connectable

      attr_reader :socket

      # Initialize the new socket with SSL.
      #
      # @example Initialize the socket.
      #   address = Address.new("127.0.0.1", 27017)
      #   SSL.new(address)
      #
      # @param [ Address ] address The address to connect to.
      #
      # @since 1.2.0
      def initialize(address)
        @address = address
        handle_socket_errors do
          if address.unix?
            @socket = UNIXSocket.new(address.unix)
          else
            @socket = TCPSocket.new(address.host, address.port)
          end
          super(socket)
          self.sync_close = true
          connect
        end
      end

      # Set the encoding of the underlying socket.
      #
      # @param [ String ] string The encoding.
      #
      # @since 1.3.0
      def set_encoding(string)
        socket.set_encoding(string)
      end

      # Set a socket option on the underlying socket.
      #
      # @param [ Array<Object> ] args The option arguments.
      #
      # @since 1.3.0
      def setsockopt(*args)
        socket.setsockopt(*args)
      end
    end
  end
end
