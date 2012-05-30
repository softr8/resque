require 'test_helper'

describe "Tr8sque::Worker" do
  include Test::Unit::Assertions

  before do
    Tr8sque.redis = Tr8sque.redis # reset state in Tr8sque object
    Tr8sque.redis.flushall

    Tr8sque.before_first_fork = nil
    Tr8sque.before_fork = nil
    Tr8sque.after_fork = nil

    @worker = Tr8sque::Worker.new(:jobs)
    Tr8sque::Job.create(:jobs, SomeJob, 20, '/tmp')
  end

  it "can fail jobs" do
    Tr8sque::Job.create(:jobs, BadJob)
    @worker.work(0)
    assert_equal 1, Tr8sque::Failure.count
  end

  it "failed jobs report exception and message" do
    Tr8sque::Job.create(:jobs, BadJobWithSyntaxError)
    @worker.work(0)
    assert_equal('SyntaxError', Tr8sque::Failure.all['exception'])
    assert_equal('Extra Bad job!', Tr8sque::Failure.all['error'])
  end

  it "does not allow exceptions from failure backend to escape" do
    job = Tr8sque::Job.new(:jobs, {})
    with_failure_backend BadFailureBackend do
      @worker.perform job
    end
  end

  it "fails uncompleted jobs on exit" do
    job = Tr8sque::Job.new(:jobs, {'class' => 'GoodJob', 'args' => "blah"})
    @worker.working_on(job)
    @worker.unregister_worker
    assert_equal 1, Tr8sque::Failure.count
  end

  class ::SimpleJobWithFailureHandling
    def self.on_failure_record_failure(exception, *job_args)
      @@exception = exception
    end
    
    def self.exception
      @@exception
    end
  end

  it "fails uncompleted jobs on exit, and calls failure hook" do
    job = Tr8sque::Job.new(:jobs, {'class' => 'SimpleJobWithFailureHandling', 'args' => ""})
    @worker.working_on(job)
    @worker.unregister_worker
    assert_equal 1, Tr8sque::Failure.count
    assert(SimpleJobWithFailureHandling.exception.kind_of?(Tr8sque::DirtyExit))
  end

  it "can peek at failed jobs" do
    10.times { Tr8sque::Job.create(:jobs, BadJob) }
    @worker.work(0)
    assert_equal 10, Tr8sque::Failure.count

    assert_equal 10, Tr8sque::Failure.all(0, 20).size
  end

  it "can clear failed jobs" do
    Tr8sque::Job.create(:jobs, BadJob)
    @worker.work(0)
    assert_equal 1, Tr8sque::Failure.count
    Tr8sque::Failure.clear
    assert_equal 0, Tr8sque::Failure.count
  end

  it "catches exceptional jobs" do
    Tr8sque::Job.create(:jobs, BadJob)
    Tr8sque::Job.create(:jobs, BadJob)
    @worker.process
    @worker.process
    @worker.process
    assert_equal 2, Tr8sque::Failure.count
  end

  it "strips whitespace from queue names" do
    queues = "critical, high, low".split(',')
    worker = Tr8sque::Worker.new(*queues)
    assert_equal %w( critical high low ), worker.queues
  end

  it "can work on multiple queues" do
    Tr8sque::Job.create(:high, GoodJob)
    Tr8sque::Job.create(:critical, GoodJob)

    worker = Tr8sque::Worker.new(:critical, :high)

    worker.process
    assert_equal 1, Tr8sque.size(:high)
    assert_equal 0, Tr8sque.size(:critical)

    worker.process
    assert_equal 0, Tr8sque.size(:high)
  end

  it "can work on all queues" do
    Tr8sque::Job.create(:high, GoodJob)
    Tr8sque::Job.create(:critical, GoodJob)
    Tr8sque::Job.create(:blahblah, GoodJob)

    worker = Tr8sque::Worker.new("*")

    worker.work(0)
    assert_equal 0, Tr8sque.size(:high)
    assert_equal 0, Tr8sque.size(:critical)
    assert_equal 0, Tr8sque.size(:blahblah)
  end

  it "can work with wildcard at the end of the list" do
    Tr8sque::Job.create(:high, GoodJob)
    Tr8sque::Job.create(:critical, GoodJob)
    Tr8sque::Job.create(:blahblah, GoodJob)
    Tr8sque::Job.create(:beer, GoodJob)

    worker = Tr8sque::Worker.new(:critical, :high, "*")

    worker.work(0)
    assert_equal 0, Tr8sque.size(:high)
    assert_equal 0, Tr8sque.size(:critical)
    assert_equal 0, Tr8sque.size(:blahblah)
    assert_equal 0, Tr8sque.size(:beer)
  end

  it "can work with wildcard at the middle of the list" do
    Tr8sque::Job.create(:high, GoodJob)
    Tr8sque::Job.create(:critical, GoodJob)
    Tr8sque::Job.create(:blahblah, GoodJob)
    Tr8sque::Job.create(:beer, GoodJob)

    worker = Tr8sque::Worker.new(:critical, "*", :high)

    worker.work(0)
    assert_equal 0, Tr8sque.size(:high)
    assert_equal 0, Tr8sque.size(:critical)
    assert_equal 0, Tr8sque.size(:blahblah)
    assert_equal 0, Tr8sque.size(:beer)
  end

  it "processes * queues in alphabetical order" do
    Tr8sque::Job.create(:high, GoodJob)
    Tr8sque::Job.create(:critical, GoodJob)
    Tr8sque::Job.create(:blahblah, GoodJob)

    worker = Tr8sque::Worker.new("*")
    processed_queues = []

    worker.work(0) do |job|
      processed_queues << job.queue
    end

    assert_equal %w( jobs high critical blahblah ).sort, processed_queues
  end

  it "can work with dynamically added queues when using wildcard" do
    worker = Tr8sque::Worker.new("*")

    assert_equal ["jobs"], Tr8sque.queues

    Tr8sque::Job.create(:high, GoodJob)
    Tr8sque::Job.create(:critical, GoodJob)
    Tr8sque::Job.create(:blahblah, GoodJob)

    processed_queues = []

    worker.work(0) do |job|
      processed_queues << job.queue
    end

    assert_equal %w( jobs high critical blahblah ).sort, processed_queues
  end

  it "has a unique id" do
    assert_equal "#{`hostname`.chomp}:#{$$}:jobs", @worker.to_s
  end

  it "complains if no queues are given" do
    assert_raise Tr8sque::NoQueueError do
      Tr8sque::Worker.new
    end
  end

  it "fails if a job class has no `perform` method" do
    worker = Tr8sque::Worker.new(:perform_less)
    Tr8sque::Job.create(:perform_less, Object)

    assert_equal 0, Tr8sque::Failure.count
    worker.work(0)
    assert_equal 1, Tr8sque::Failure.count
  end

  it "inserts itself into the 'workers' list on startup" do
    @worker.work(0) do
      assert_equal @worker, Tr8sque.workers[0]
    end
  end

  it "removes itself from the 'workers' list on shutdown" do
    @worker.work(0) do
      assert_equal @worker, Tr8sque.workers[0]
    end

    assert_equal [], Tr8sque.workers
  end

  it "removes worker with stringified id" do
    @worker.work(0) do
      worker_id = Tr8sque.workers[0].to_s
      Tr8sque.remove_worker(worker_id)
      assert_equal [], Tr8sque.workers
    end
  end

  it "records what it is working on" do
    @worker.work(0) do
      task = @worker.job
      assert_equal({"args"=>[20, "/tmp"], "class"=>"SomeJob"}, task['payload'])
      assert task['run_at']
      assert_equal 'jobs', task['queue']
    end
  end

  it "clears its status when not working on anything" do
    @worker.work(0)
    assert_equal Hash.new, @worker.job
  end

  it "knows when it is working" do
    @worker.work(0) do
      assert @worker.working?
    end
  end

  it "knows when it is idle" do
    @worker.work(0)
    assert @worker.idle?
  end

  it "knows who is working" do
    @worker.work(0) do
      assert_equal [@worker], Tr8sque.working
    end
  end

  it "keeps track of how many jobs it has processed" do
    Tr8sque::Job.create(:jobs, BadJob)
    Tr8sque::Job.create(:jobs, BadJob)

    3.times do
      job = @worker.reserve
      @worker.process job
    end
    assert_equal 3, @worker.processed
  end

  it "reserve blocks when the queue is empty" do
    worker = Tr8sque::Worker.new(:timeout)

    assert_raises Timeout::Error do
      Timeout.timeout(1) { worker.reserve(5) }
    end
  end

  it "reserve returns nil when there is no job and is polling" do
    worker = Tr8sque::Worker.new(:timeout)

    assert_equal nil, worker.reserve(1)
  end

  it "keeps track of how many failures it has seen" do
    Tr8sque::Job.create(:jobs, BadJob)
    Tr8sque::Job.create(:jobs, BadJob)

    3.times do
      job = @worker.reserve
      @worker.process job
    end
    assert_equal 2, @worker.failed
  end

  it "stats are erased when the worker goes away" do
    @worker.work(0)
    assert_equal 0, @worker.processed
    assert_equal 0, @worker.failed
  end

  it "knows when it started" do
    time = Time.now
    @worker.work(0) do
      assert_equal time.to_s, @worker.started.to_s
    end
  end

  it "knows whether it exists or not" do
    @worker.work(0) do
      assert Tr8sque::Worker.exists?(@worker)
      assert !Tr8sque::Worker.exists?('blah-blah')
    end
  end

  it "sets $0 while working" do
    @worker.work(0) do
      ver = Tr8sque::Version
      assert_equal "resque-#{ver}: Processing jobs since #{Time.now.to_i}", $0
    end
  end

  it "can be found" do
    @worker.work(0) do
      found = Tr8sque::Worker.find(@worker.to_s)
      assert_equal @worker.to_s, found.to_s
      assert found.working?
      assert_equal @worker.job, found.job
    end
  end

  it "doesn't find fakes" do
    @worker.work(0) do
      found = Tr8sque::Worker.find('blah-blah')
      assert_equal nil, found
    end
  end

  it "cleans up dead worker info on start (crash recovery)" do
    # first we fake out two dead workers
    workerA = Tr8sque::Worker.new(:jobs)
    workerA.instance_variable_set(:@to_s, "#{`hostname`.chomp}:1:jobs")
    workerA.register_worker

    workerB = Tr8sque::Worker.new(:high, :low)
    workerB.instance_variable_set(:@to_s, "#{`hostname`.chomp}:2:high,low")
    workerB.register_worker

    assert_equal 2, Tr8sque.workers.size

    # then we prune them
    @worker.work(0) do
      assert_equal 1, Tr8sque.workers.size
    end
  end

  it "worker_pids returns pids" do
    known_workers = @worker.worker_pids
    assert !known_workers.empty?
  end

  it "Processed jobs count" do
    @worker.work(0)
    assert_equal 1, Tr8sque.info[:processed]
  end

  it "Will call a before_first_fork hook only once" do
    $BEFORE_FORK_CALLED = 0
    Tr8sque.before_first_fork = Proc.new { $BEFORE_FORK_CALLED += 1 }
    workerA = Tr8sque::Worker.new(:jobs)
    Tr8sque::Job.create(:jobs, SomeJob, 20, '/tmp')

    assert_equal 0, $BEFORE_FORK_CALLED

    workerA.work(0)
    assert_equal 1, $BEFORE_FORK_CALLED

    # TODO: Verify it's only run once. Not easy.
#     workerA.work(0)
#     assert_equal 1, $BEFORE_FORK_CALLED
  end

  it "Will call a before_fork hook before forking" do
    $BEFORE_FORK_CALLED = false
    Tr8sque.before_fork = Proc.new { $BEFORE_FORK_CALLED = true }
    workerA = Tr8sque::Worker.new(:jobs)

    assert !$BEFORE_FORK_CALLED
    Tr8sque::Job.create(:jobs, SomeJob, 20, '/tmp')
    workerA.work(0)
    assert $BEFORE_FORK_CALLED
  end

  it "very verbose works in the afternoon" do
    begin
      require 'time'
      last_puts = ""
      Time.fake_time = Time.parse("15:44:33 2011-03-02")

      @worker.extend(Module.new {
        define_method(:puts) { |thing| last_puts = thing }
      })

      @worker.very_verbose = true
      @worker.log("some log text")

      assert_match /\*\* \[15:44:33 2011-03-02\] \d+: some log text/, last_puts
    ensure
      Time.fake_time = nil
    end
  end

  it "Will call an after_fork hook after forking" do
    $AFTER_FORK_CALLED = false
    Tr8sque.after_fork = Proc.new { $AFTER_FORK_CALLED = true }
    workerA = Tr8sque::Worker.new(:jobs)

    assert !$AFTER_FORK_CALLED
    Tr8sque::Job.create(:jobs, SomeJob, 20, '/tmp')
    workerA.work(0)
    assert $AFTER_FORK_CALLED
  end

  it "returns PID of running process" do
    assert_equal @worker.to_s.split(":")[1].to_i, @worker.pid
  end
  
  it "requeue failed queue" do
    queue = 'good_job'
    Tr8sque::Failure.create(:exception => Exception.new, :worker => Tr8sque::Worker.new(queue), :queue => queue, :payload => {'class' => 'GoodJob'})
    Tr8sque::Failure.create(:exception => Exception.new, :worker => Tr8sque::Worker.new(queue), :queue => 'some_job', :payload => {'class' => 'SomeJob'})
    Tr8sque::Failure.requeue_queue(queue)
    assert Tr8sque::Failure.all(0).has_key?('retried_at')
    assert !Tr8sque::Failure.all(1).has_key?('retried_at')
  end

  it "remove failed queue" do
    queue = 'good_job'
    queue2 = 'some_job'
    Tr8sque::Failure.create(:exception => Exception.new, :worker => Tr8sque::Worker.new(queue), :queue => queue, :payload => {'class' => 'GoodJob'})
    Tr8sque::Failure.create(:exception => Exception.new, :worker => Tr8sque::Worker.new(queue2), :queue => queue2, :payload => {'class' => 'SomeJob'})
    Tr8sque::Failure.create(:exception => Exception.new, :worker => Tr8sque::Worker.new(queue), :queue => queue, :payload => {'class' => 'GoodJob'})
    Tr8sque::Failure.remove_queue(queue)
    assert_equal queue2, Tr8sque::Failure.all(0)['queue']
    assert_equal 1, Tr8sque::Failure.count
  end
end
