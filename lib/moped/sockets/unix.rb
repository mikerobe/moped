module Moped
  module Sockets

    # This is a wrapper around a tcp socket.
    class UNIX < ::UNIXSocket
      include Connectable

      # Initialize the new UNIXSocket.
      #
      # @example Initialize the socket.
      #   address = Address.new("127.0.0.1", 27017)
      #   UNIXSocket.new(address)
      #
      # @param [ Address ] address The address to connect to.
      #
      # @since 1.2.0
      def initialize(address)
        @address = address
        handle_socket_errors { super address.unix }
      end
    end
  end
end
