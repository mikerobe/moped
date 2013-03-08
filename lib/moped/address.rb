# encoding: utf-8
module Moped

  # Encapsulates behaviour around addresses and resolving dns.
  #
  # @since 2.0.0
  class Address

    # @!attribute host
    #   @return [ String ] The host name.
    # @!attribute ip
    #   @return [ String ] The ip address.
    # @!attribute original
    #   @return [ String ] The original host name.
    # @!attribute port
    #   @return [ Integer ] The port.
    # @!attribute resolved
    #   @return [ String ] The full resolved address.
    # @!attribute unix
    #   @return [ String ] The unix socket path, if applicable
    attr_reader :host, :ip, :original, :port, :resolved, :unix

    # Instantiate the new address.
    #
    # @example Instantiate the address.
    #   Moped::Address.new("localhost:27017")
    #
    # @param [ String ] address The host:port pair as a string.
    #
    # @since 2.0.0
    def initialize(address)
      @original = address
      if File.socket? address or /^\// === address
        @unix = address
      else
        @host, port = address.split(":")
        @port = (port || 27017).to_i
      end
    end

    # Resolve the address for the provided node. If the address cannot be
    # resolved the node will be flagged as down.
    #
    # @example Resolve the address.
    #   address.resolve(node)
    #
    # @param [ Node ] node The node to resolve for.
    #
    # @return [ String ] The resolved address.
    #
    # @since 2.0.0
    def resolve(node)
      begin
        if unix?
          @ip ||= '127.0.0.1'
          @resolved ||= unix
        else
          @ip ||= Socket.getaddrinfo(host, nil, Socket::AF_INET, Socket::SOCK_STREAM).first[3]
          @resolved ||= "#{ip}:#{port}"
        end
      rescue SocketError => e
        node.instrument(Node::WARN, prefix: "  MOPED:", message: "Could not resolve IP for: #{original}")
        node.down! and false
      end
    end

    # Get the string representation of the address. If it has been resolved,
    # returns the resolved address, otherwise the original address specified in
    # the constructor.
    #
    # @example Get the string representation.
    #   address.to_s
    #
    # @return [ String ] The string representation.
    def to_s
      resolved || original
    end

    # Returns true if this address refers to a unix domain socket, rather than
    # a TCP socket. Unix domain sockets are identified as paths on the local
    # file system, rather than with a host and port.
    #
    # @example Check if the address is unix
    #   if address.unix?
    #     ...
    #   end
    #
    # @return [ true, false ] True if this addresses a unix domain socket
    def unix?
      not unix.nil?
    end

  end
end
