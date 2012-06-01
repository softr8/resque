require 'test_helper'

describe "Tr8sque" do
  include Test::Unit::Assertions

  before do
    Tr8sque.redis.flushall

    Tr8sque.push(:people, { 'name' => 'chris' })
    Tr8sque.push(:people, { 'name' => 'bob' })
    Tr8sque.push(:people, { 'name' => 'mark' })
    @original_redis = Tr8sque.redis
  end

  after do
    Tr8sque.redis = @original_redis
  end

  it "can set a namespace through a url-like string" do
    assert Tr8sque.redis
    assert_equal :resque, Tr8sque.redis.namespace
    Tr8sque.redis = 'localhost:9736/namespace'
    assert_equal 'namespace', Tr8sque.redis.namespace
  end

  it "redis= works correctly with a Tr8dis::Namespace param" do
    new_redis = Tr8dis.new(:host => "localhost", :port => 9736)
    new_namespace = Tr8dis::Namespace.new("namespace", :redis => new_redis)
    Tr8sque.redis = new_namespace
    assert_equal new_namespace, Tr8sque.redis

    Tr8sque.redis = 'localhost:9736/namespace'
  end

  it "can put jobs on a queue" do
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
  end

  it "can grab jobs off a queue" do
    Tr8sque::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = Tr8sque.reserve(:jobs)

    assert_kind_of Tr8sque::Job, job
    assert_equal SomeJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]
  end

  it "can re-queue jobs" do
    Tr8sque::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = Tr8sque.reserve(:jobs)
    job.recreate

    assert_equal job, Tr8sque.reserve(:jobs)
  end

  it "can put jobs on a queue by way of an ivar" do
    assert_equal 0, Tr8sque.size(:ivar)
    assert Tr8sque.enqueue(SomeIvarJob, 20, '/tmp')
    assert Tr8sque.enqueue(SomeIvarJob, 20, '/tmp')

    job = Tr8sque.reserve(:ivar)

    assert_kind_of Tr8sque::Job, job
    assert_equal SomeIvarJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]

    assert Tr8sque.reserve(:ivar)
    assert_equal nil, Tr8sque.reserve(:ivar)
  end

  it "can remove jobs from a queue by way of an ivar" do
    assert_equal 0, Tr8sque.size(:ivar)
    assert Tr8sque.enqueue(SomeIvarJob, 20, '/tmp')
    assert Tr8sque.enqueue(SomeIvarJob, 30, '/tmp')
    assert Tr8sque.enqueue(SomeIvarJob, 20, '/tmp')
    assert Tr8sque::Job.create(:ivar, 'blah-job', 20, '/tmp')
    assert Tr8sque.enqueue(SomeIvarJob, 20, '/tmp')
    assert_equal 5, Tr8sque.size(:ivar)

    assert Tr8sque.dequeue(SomeIvarJob, 30, '/tmp')
    assert_equal 4, Tr8sque.size(:ivar)
    assert Tr8sque.dequeue(SomeIvarJob)
    assert_equal 1, Tr8sque.size(:ivar)
  end

  it "jobs have a nice #inspect" do
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    job = Tr8sque.reserve(:jobs)
    assert_equal '(Job{jobs} | SomeJob | [20, "/tmp"])', job.inspect
  end

  it "jobs can be destroyed" do
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'BadJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'BadJob', 30, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'BadJob', 20, '/tmp')

    assert_equal 5, Tr8sque.size(:jobs)
    assert_equal 2, Tr8sque::Job.destroy(:jobs, 'SomeJob')
    assert_equal 3, Tr8sque.size(:jobs)
    assert_equal 1, Tr8sque::Job.destroy(:jobs, 'BadJob', 30, '/tmp')
    assert_equal 2, Tr8sque.size(:jobs)
  end

  it "jobs can it for equality" do
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'some-job', 20, '/tmp')
    assert_equal Tr8sque.reserve(:jobs), Tr8sque.reserve(:jobs)

    assert Tr8sque::Job.create(:jobs, 'SomeMethodJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert_not_equal Tr8sque.reserve(:jobs), Tr8sque.reserve(:jobs)

    assert Tr8sque::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert Tr8sque::Job.create(:jobs, 'SomeJob', 30, '/tmp')
    assert_not_equal Tr8sque.reserve(:jobs), Tr8sque.reserve(:jobs)
  end

  it "can put jobs on a queue by way of a method" do
    assert_equal 0, Tr8sque.size(:method)
    assert Tr8sque.enqueue(SomeMethodJob, 20, '/tmp')
    assert Tr8sque.enqueue(SomeMethodJob, 20, '/tmp')

    job = Tr8sque.reserve(:method)

    assert_kind_of Tr8sque::Job, job
    assert_equal SomeMethodJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]

    assert Tr8sque.reserve(:method)
    assert_equal nil, Tr8sque.reserve(:method)
  end

  it "can define a queue for jobs by way of a method" do
    assert_equal 0, Tr8sque.size(:method)
    assert Tr8sque.enqueue_to(:new_queue, SomeMethodJob, 20, '/tmp')

    job = Tr8sque.reserve(:new_queue)
    assert_equal SomeMethodJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]
  end

  it "needs to infer a queue with enqueue" do
    assert_raises Tr8sque::NoQueueError do
      Tr8sque.enqueue(SomeJob, 20, '/tmp')
    end
  end

  it "validates job for queue presence" do
    assert_raises Tr8sque::NoQueueError do
      Tr8sque.validate(SomeJob)
    end
  end

  it "can put items on a queue" do
    assert Tr8sque.push(:people, { 'name' => 'jon' })
  end

  it "can pull items off a queue" do
    assert_equal({ 'name' => 'chris' }, Tr8sque.pop(:people))
    assert_equal({ 'name' => 'bob' }, Tr8sque.pop(:people))
    assert_equal({ 'name' => 'mark' }, Tr8sque.pop(:people))
    assert_equal nil, Tr8sque.pop(:people)
  end

  it "knows how big a queue is" do
    assert_equal 3, Tr8sque.size(:people)

    assert_equal({ 'name' => 'chris' }, Tr8sque.pop(:people))
    assert_equal 2, Tr8sque.size(:people)

    assert_equal({ 'name' => 'bob' }, Tr8sque.pop(:people))
    assert_equal({ 'name' => 'mark' }, Tr8sque.pop(:people))
    assert_equal 0, Tr8sque.size(:people)
  end

  it "can peek at a queue" do
    assert_equal({ 'name' => 'chris' }, Tr8sque.peek(:people))
    assert_equal 3, Tr8sque.size(:people)
  end

  it "can peek multiple items on a queue" do
    assert_equal({ 'name' => 'bob' }, Tr8sque.peek(:people, 1, 1))

    assert_equal([{ 'name' => 'bob' }, { 'name' => 'mark' }], Tr8sque.peek(:people, 1, 2))
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }], Tr8sque.peek(:people, 0, 2))
    assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }, { 'name' => 'mark' }], Tr8sque.peek(:people, 0, 3))
    assert_equal({ 'name' => 'mark' }, Tr8sque.peek(:people, 2, 1))
    assert_equal nil, Tr8sque.peek(:people, 3)
    assert_equal [], Tr8sque.peek(:people, 3, 2)
  end

  it "knows what queues it is managing" do
    assert_equal %w( people ), Tr8sque.queues
    Tr8sque.push(:cars, { 'make' => 'bmw' })
    assert_equal %w( cars people ), Tr8sque.queues
  end

  it "queues are always a list" do
    Tr8sque.redis.flushall
    assert_equal [], Tr8sque.queues
  end

  it "can delete a queue" do
    Tr8sque.push(:cars, { 'make' => 'bmw' })
    assert_equal %w( cars people ), Tr8sque.queues
    Tr8sque.remove_queue(:people)
    assert_equal %w( cars ), Tr8sque.queues
    assert_equal nil, Tr8sque.pop(:people)
  end

  it "keeps track of resque keys" do
    assert_equal ["queue:people", "queues"].sort, Tr8sque.keys.sort
  end

  it "badly wants a class name, too" do
    assert_raises Tr8sque::NoClassError do
      Tr8sque::Job.create(:jobs, nil)
    end
  end

  it "keeps stats" do
    Tr8sque::Job.create(:jobs, SomeJob, 20, '/tmp')
    Tr8sque::Job.create(:jobs, BadJob)
    Tr8sque::Job.create(:jobs, GoodJob)

    Tr8sque::Job.create(:others, GoodJob)
    Tr8sque::Job.create(:others, GoodJob)

    stats = Tr8sque.info
    assert_equal 8, stats[:pending]

    @worker = Tr8sque::Worker.new(:jobs)
    @worker.register_worker
    2.times { @worker.process }

    job = @worker.reserve
    @worker.working_on job

    stats = Tr8sque.info
    assert_equal 1, stats[:working]
    assert_equal 1, stats[:workers]

    @worker.done_working

    stats = Tr8sque.info
    assert_equal 3, stats[:queues]
    assert_equal 3, stats[:processed]
    assert_equal 1, stats[:failed]
    if ENV.key? 'RESQUE_DISTRIBUTED'
      assert_equal [Tr8sque.redis.respond_to?(:server) ? 'localhost:9736, localhost:9737' : 'redis://localhost:9736/0, redis://localhost:9737/0'], stats[:servers]
    else
      assert_equal [Tr8sque.redis.respond_to?(:server) ? 'localhost:9736' : 'redis://localhost:9736/0'], stats[:servers]
    end
  end

  it "decode bad json" do
    assert_raises Tr8sque::DecodeException do
      Tr8sque.coder.decode("{\"error\":\"Module not found \\u002\"}")
    end
  end

  it "inlining jobs" do
    begin
      Tr8sque.inline = true
      Tr8sque.enqueue(SomeIvarJob, 20, '/tmp')
      assert_equal 0, Tr8sque.size(:ivar)
    ensure
      Tr8sque.inline = false
    end
  end

  it 'treats symbols and strings the same' do
    assert_equal Tr8sque.queue(:people), Tr8sque.queue('people')
  end
end
