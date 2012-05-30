begin
  require 'hoptoad_notifier'
rescue LoadError
  raise "Can't find 'hoptoad_notifier' gem. Please add it to your Gemfile or install it."
end

require 'tr8sque/failure/thoughtbot'

module Tr8sque
  module Failure
    # A Failure backend that sends exceptions raised by jobs to Hoptoad.
    #
    # To use it, put this code in an initializer, Rake task, or wherever:
    #
    #   require 'tr8sque/failure/hoptoad'
    #
    #   Tr8sque::Failure::Multiple.classes = [Tr8sque::Failure::Redis, Tr8sque::Failure::Hoptoad]
    #   Tr8sque::Failure.backend = Tr8sque::Failure::Multiple
    #
    # Once you've configured resque to use the Hoptoad failure backend,
    # you'll want to setup an initializer to configure the Hoptoad.
    #
    # HoptoadNotifier.configure do |config|
    #   config.api_key = 'your_key_here'
    # end
    # For more information see https://github.com/thoughtbot/hoptoad_notifier
    class Hoptoad < Base
      include Tr8sque::Failure::Thoughtbot

      @klass = ::HoptoadNotifier
    end
  end
end
