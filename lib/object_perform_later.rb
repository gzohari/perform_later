module ObjectPerformLater
  def perform_later(queue, method, *args)
    return perform_now(method, args) if plugin_disabled?

    worker = PerformLater::Workers::Objects::Worker
    job    = PerformLater::JobCreator.new({queue: queue, worker: worker, klass_name: self.name, method: method}, *args)
    perform_later_enqueue(job)
  end

  def perform_later!(queue, method, *args)
    return perform_now(method, args) if plugin_disabled?

    return "EXISTS!" if loner_exists(method, args)

    worker = PerformLater::Workers::Objects::LoneWorker
    job    = PerformLater::JobCreator.new({queue: queue, worker: worker, klass_name: self.name, method: method}, *args)
    perform_later_enqueue(job)
  end

  def perform_later_in(delay, queue, method, *args)
    return perform_now(method, args) if plugin_disabled?

    worker  = PerformLater::Workers::ActiveRecord::Worker
    job     = PerformLater::JobCreator.new({queue: queue, worker: worker, klass_name: self.name, method: method}, *args) 
    perform_later_enqueue(job, delay)
  end
  
  def perform_later_in!(delay, queue, method, *args)
    return  perform_now(method, args) if plugin_disabled?

    worker  = PerformLater::Workers::ActiveRecord::LoneWorker
    job     = PerformLater::JobCreator.new({queue: queue, worker: worker, klass_name: self.name, method: method}, *args) 
    perform_later_enqueue(job, delay)
  end


  private 
    def loner_exists(method, *args)
      digest = PerformLater::PayloadHelper.get_digest(self.name, method, args)

      return true unless Resque.redis.get(digest).blank?
      Resque.redis.set(digest, 'EXISTS')
      return false
    end

    def perform_later_enqueue(job,delay=nil)
      job.args = PerformLater::ArgsParser.args_to_resque(job.args)
      job.enqueue(delay)
    end

    def perform_now(method, args)
      args.size == 1 ? send(method, args.first) : send(method, *args)
    end

    def plugin_disabled?
      !PerformLater.config.enabled?
    end

end

Object.send(:include, ObjectPerformLater)
