require 'rack/test'
require 'tr8sque/server'

module Tr8sque
  module TestHelper
    class MiniTest::Unit::TestCase
      include Rack::Test::Methods
      def app
        Tr8sque::Server.new
      end 

      def self.should_respond_with_success
        it "should respond with success" do
          assert last_response.ok?, last_response.errors
        end
      end
    end
  end
end
