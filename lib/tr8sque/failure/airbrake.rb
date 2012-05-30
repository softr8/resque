begin
  require 'airbrake'
rescue LoadError
  raise "Can't find 'airbrake' gem. Please add it to your Gemfile or install it."
end

require 'tr8sque/failure/thoughtbot'

module Tr8sque
  module Failure
    class Airbrake < Base
      include Tr8sque::Failure::Thoughtbot

      @klass = ::Airbrake
    end
  end
end
