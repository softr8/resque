module Tr8sque
  module Failure
    # A Failure backend that stores exceptions in Redis. Very simple but
    # works out of the box, along with support in the Tr8sque web app.
    class Redis < Base
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S %Z"),
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => filter_backtrace(Array(exception.backtrace)),
          :worker    => worker.to_s,
          :queue     => queue
        }
        data = Tr8sque.encode(data)
        Tr8sque.redis.rpush(:failed, data)
      end

      def self.count
        Tr8sque.redis.llen(:failed).to_i
      end

      def self.all(start = 0, count = 1)
        Tr8sque.list_range(:failed, start, count)
      end

      def self.clear
        Tr8sque.redis.del(:failed)
      end

      def self.requeue(index)
        item = all(index)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Tr8sque.redis.lset(:failed, index, Tr8sque.encode(item))
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end

      def self.remove(index)
        id = rand(0xffffff)
        Tr8sque.redis.lset(:failed, index, id)
        Tr8sque.redis.lrem(:failed, 1, id)
      end

      def filter_backtrace(backtrace)
        index = backtrace.index { |item| item.include?('/lib/resque/job.rb') }
        backtrace.first(index.to_i)
      end
    end
  end
end
