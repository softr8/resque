require "test_helper"

describe "Tr8sque::MulitQueue" do
  let(:redis) { Tr8sque.redis }
  let(:coder) { Tr8sque::MultiJsonCoder.new }

  before do
    redis.flushall
  end

  it "poll times out and returns nil" do
    foo   = Tr8sque::Queue.new 'foo', redis
    bar   = Tr8sque::Queue.new 'bar', redis
    queue = Tr8sque::MultiQueue.new([foo, bar], redis)
    assert_nil queue.poll(1)
  end

  it "blocks on pop" do
    foo   = Tr8sque::Queue.new 'foo', redis, coder
    bar   = Tr8sque::Queue.new 'bar', redis, coder
    queue = Tr8sque::MultiQueue.new([foo, bar], redis)
    t     = Thread.new { queue.pop }

    job = { 'class' => 'GoodJob', 'args' => [35, 'tar'] }
    bar << job

    assert_equal [bar, job], t.join.value
  end

  it "nonblocking pop works" do
    foo   = Tr8sque::Queue.new 'foo', redis, coder
    bar   = Tr8sque::Queue.new 'bar', redis, coder
    queue = Tr8sque::MultiQueue.new([foo, bar], redis)

    job = { 'class' => 'GoodJob', 'args' => [35, 'tar'] }
    bar << job

    assert_equal [bar, job], queue.pop(true)
  end

  it "nonblocking pop doesn't block" do
    foo   = Tr8sque::Queue.new 'foo', redis, coder
    bar   = Tr8sque::Queue.new 'bar', redis, coder
    queue = Tr8sque::MultiQueue.new([foo, bar], redis)

    assert_raises ThreadError do
      queue.pop(true)
    end
  end

  it "blocks forever on pop" do
    foo   = Tr8sque::Queue.new 'foo', redis, coder
    bar   = Tr8sque::Queue.new 'bar', redis, coder
    queue = Tr8sque::MultiQueue.new([foo, bar], redis)
    assert_raises Timeout::Error do
      Timeout::timeout(2) { queue.pop }
    end
  end

  it "blocking pop processes queues in the order given" do
    foo    = Tr8sque::Queue.new 'foo', redis, coder
    bar    = Tr8sque::Queue.new 'bar', redis, coder
    baz    = Tr8sque::Queue.new 'baz', redis, coder
    queues = [foo, bar, baz]
    queue  = Tr8sque::MultiQueue.new(queues, redis)
    job    = { 'class' => 'GoodJob', 'args' => [35, 'tar'] }

    queues.each {|q| q << job }

    processed_queues = queues.map do
      q, j = queue.pop
      q
    end

    assert_equal processed_queues, queues
  end

  it "nonblocking pop processes queues in the order given" do
    foo    = Tr8sque::Queue.new 'foo', redis, coder
    bar    = Tr8sque::Queue.new 'bar', redis, coder
    baz    = Tr8sque::Queue.new 'baz', redis, coder
    queues = [foo, bar, baz]
    queue  = Tr8sque::MultiQueue.new(queues, redis)
    job    = { 'class' => 'GoodJob', 'args' => [35, 'tar'] }

    queues.each {|q| q << job }

    processed_queues = queues.map do
      q, j = queue.pop(true)
      q
    end

    assert_equal processed_queues, queues
  end
end
