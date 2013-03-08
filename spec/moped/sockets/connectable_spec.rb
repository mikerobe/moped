require "spec_helper"

describe Moped::Sockets::Connectable do

  describe "#handle_socket_errors" do

    let(:object) do
      Class.new do
        include Moped::Sockets::Connectable
        def address; "127.0.0.1:27017" end
      end.new
    end

    context "when a Errno::ECONNREFUSED is raised" do

      it "re-raises a ConnectionFailure" do
        expect{
          object.send(:handle_socket_errors) { raise Errno::ECONNREFUSED }
        }.to raise_error(
          Moped::Errors::ConnectionFailure
        )
      end
    end

    context "when a Errno::EHOSTUNREACH is raised" do

      it "re-raises a ConnectionFailure" do
        expect{
          object.send(:handle_socket_errors) { raise Errno::EHOSTUNREACH }
        }.to raise_error(
          Moped::Errors::ConnectionFailure
        )
      end
    end

    context "when a Errno::EPIPE is raised" do

      it "re-raises a ConnectionFailure" do
        expect{
          object.send(:handle_socket_errors) { raise Errno::EPIPE }
        }.to raise_error(
          Moped::Errors::ConnectionFailure
        )
      end
    end

    context "when a Errno::ECONNRESET is raised" do

      it "re-raises a ConnectionFailure" do
        expect{
          object.send(:handle_socket_errors) { raise Errno::ECONNRESET }
        }.to raise_error(
          Moped::Errors::ConnectionFailure
        )
      end
    end

    context "when a Errno::ETIMEDOUT is raised" do

      it "re-raises a ConnectionFailure" do
        expect{
          object.send(:handle_socket_errors) { raise Errno::ETIMEDOUT }
        }.to raise_error(
          Moped::Errors::ConnectionFailure
        )
      end
    end
  end
end
