# require 'tr8sque/tasks'
# will give you the resque tasks

namespace :tr8sque do
  task :setup

  desc "Start a Tr8sque worker"
  task :work => [ :preload, :setup ] do
    require 'tr8sque'

    queues = (ENV['QUEUES'] || ENV['QUEUE']).to_s.split(',')

    begin
      worker = Tr8sque::Worker.new(*queues)
      worker.verbose = ENV['LOGGING'] || ENV['VERBOSE']
      worker.very_verbose = ENV['VVERBOSE']
    rescue Tr8sque::NoQueueError
      abort "set QUEUE env var, e.g. $ QUEUE=critical,high rake resque:work"
    end

    if ENV['BACKGROUND']
      unless Process.respond_to?('daemon')
          abort "env var BACKGROUND is set, which requires ruby >= 1.9"
      end
      Process.daemon(true)
    end

    if ENV['PIDFILE']
      File.open(ENV['PIDFILE'], 'w') { |f| f << worker.pid }
    end

    worker.log "Starting worker #{worker}"

    worker.work(ENV['INTERVAL'] || 5) # interval, will block
  end

  desc "Start multiple Tr8sque workers. Should only be used in dev mode."
  task :workers do
    threads = []

    ENV['COUNT'].to_i.times do
      threads << Thread.new do
        system "rake tr8sque:work"
      end
    end

    threads.each { |thread| thread.join }
  end

  # Preload app files if this is Rails
  task :preload => :setup do
    if defined?(Rails) && Rails.respond_to?(:application)
      # Rails 3
      Rails.application.eager_load!
    elsif defined?(Rails::Initializer)
      # Rails 2.3
      $rails_rake_task = false
      Rails::Initializer.run :load_application_classes
    end
  end
end
