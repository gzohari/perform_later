module PerformLater
  class JobCreator
    
    attr_reader :queue, :worker, :klass_name, :id, :method
    attr_accessor :args

    def initialize(opts, *args)
      defaults = {id: nil}
      defaults.merge!(opts)

      @queue       = opts[:queue]
      @worker      = opts[:worker]
      @klass_name  = opts[:klass_name]
      @id          = opts[:id]
      @method      = opts[:method]
      @args        = args
    end

    def enqueue(delay=nil)
      return Resque.enqueue_in_with_queue(queue, delay, worker, klass_name, id, method, *args) if delay 
      Resque::Job.create(queue, worker, klass_name, id, method, *args)
    end
  end
end
