using System.Text;

namespace JamilTero_B2BrokerClassLibrary
{
    public class MessageRepo
    {
        private string _Id;
        private Logger _logger;
        private Queue<byte[]> _queue;
        private int _queueSize = 0;
        public MessageRepo(Logger logger, string id)
        {
            _queue = new Queue<byte[]>();
            _queueSize = 0;
            _logger = logger;
            _Id = "MessageRepo_" + id;
        }
        public async Task receiveMessage(byte[] nextMessage)
        {
            _logger.logMessage(string.Format("instance: {0}, action: receive, msg: {1}, qsize: {2}", _Id
                    , Convert.ToBase64String(nextMessage), _queueSize));
            _queue.Enqueue(nextMessage);
            _queueSize += nextMessage.Length;
        }
        public byte[] pop()
        {
            byte[] msg = _queue.Dequeue();
            _queueSize -= msg.Length;
            _logger.logMessage(string.Format("instance: {0}, action: pop, msg: {1}, qsize: {2}", _Id
                   , Convert.ToBase64String(msg), _queueSize));
            return msg;
        }
        public int getQueueSize() => _queueSize;

    }
    public class Manager : IAsyncDisposable
    {
        private string _id;
        private Logger _logger;
        private MessageRepo _repo;
        private Publisher _publisher;
        private readonly MemoryStream _buffer = new();
        private readonly SemaphoreSlim _receiverLock;
        private object _bufferLock = new object();
        protected bool _bufferInHold = false;
        private readonly int _bufferThreshold;
        public Manager(IBusConnection publisherConnection, Logger logger, string id, int bufferThreshold)
        {
            _id = "Meanager_" + id;
            _logger = logger;
            _publisher = new Publisher(publisherConnection);
            _repo = new MessageRepo(_logger, id);
            _buffer = new();
            _bufferLock = new object();
            _receiverLock = new SemaphoreSlim(1, 1);
            _bufferInHold = false;
            _bufferThreshold = 10;
            _bufferThreshold = bufferThreshold;
        }
        public async Task SendMessageAsync(byte[] nextMessage)
        {
            try
            {
                await _receiverLock.WaitAsync();
                await _repo.receiveMessage(nextMessage);
                if (this.shouldPackage())
                    await package();
            }
            finally
            {
                _receiverLock.Release();
            }
        }
        protected bool shouldPackage() => _repo.getQueueSize() > _bufferThreshold && !_bufferInHold;
        protected async Task package()
        {
            if (!_bufferInHold)
            {
                lock (_bufferLock)
                {
                    _bufferInHold = true;
                    while (_buffer.Length <= _bufferThreshold && _repo.getQueueSize() > 0)
                    {
                        byte[] msg = _repo.pop();
                        _buffer.Write(msg, 0, msg.Length);
                        _logger.logMessage(string.Format("{0},message to buffer, msg: {1}, buffer:{2}, queue size:{3}",
                            _id, Convert.ToBase64String(msg), Convert.ToBase64String(_buffer.ToArray())
                            , _repo.getQueueSize()));
                    }
                    _logger.logMessage(string.Format("{0},call publisher, buffer: {1}", _id, Convert.ToBase64String(_buffer.ToArray())));
                    Task.Run(() => _publisher.publish(getBuffer()).ContinueWith(task =>
                    { releaseBuffer(); }));
                }
            }
        }
        protected byte[] getBuffer() => this._buffer.ToArray();
        protected void releaseBuffer()
        {
            _bufferInHold = false;
            _buffer.SetLength(0);
            _logger.logMessage(string.Format("{0}, release buffer, queue size: {1}", _id, _repo.getQueueSize()));
            if (shouldPackage())
                package();
        }

        public bool workInProgress() => this._bufferInHold || _repo.getQueueSize() > _bufferThreshold;

        public async ValueTask DisposeAsync()
        {
            Console.WriteLine($"dispose called: {_repo.getQueueSize()},{_bufferInHold}");
            int tryCount = 0;
            while (_bufferInHold && tryCount < 30)
            {
                Thread.Sleep(10);
                tryCount++;
            }
            if (!_bufferInHold)
            {
                _bufferInHold = true;
                while (_repo.getQueueSize() > 0)
                {
                    byte[] msg = _repo.pop();
                    _buffer.Write(msg, 0, msg.Length);
                    _logger.logMessage(string.Format("{0},message to buffer, msg: {1}, buffer:{2}, queue size:{3}",
                        _id, Convert.ToBase64String(msg), Convert.ToBase64String(_buffer.ToArray())
                        , _repo.getQueueSize()));
                }
                _logger.logMessage(string.Format("{0},call publisher, buffer: {1}", _id, Convert.ToBase64String(_buffer.ToArray())));
                await _publisher.publish(getBuffer());
                releaseBuffer(); ;
            }
            else
            {
                //throw exception can't commit remaining records
            }
        }
    }
    public class Publisher
    {
        private readonly IBusConnection _connection;
        private readonly SemaphoreSlim _publishLock;
        public Publisher(IBusConnection connection)
        {
            _connection = connection;
            _publishLock = new SemaphoreSlim(1, 1);
        }
        internal async Task publish(byte[] package)
        {
            await _publishLock.WaitAsync();
            try
            {
                await _connection.PublishAsync(package);
            }
            finally
            {
                _publishLock.Release();
            }
        }
    }
    public class Logger
    {
        private StringBuilder _log;
        private bool _isEnabled;
        public Logger(bool isenabled = true)
        {
            _isEnabled = isenabled;
            _log = new StringBuilder();
        }

        public void logMessage(string msg)
        {
            if (_isEnabled)
            {
                Console.WriteLine(msg);
                _log.Append(msg);
            }

        }
        public string copyLog() => _isEnabled ? _log.ToString() : string.Empty;
        public void printLog()
        {
            if (_isEnabled)
                Console.WriteLine(_log.ToString());
            else
                Console.WriteLine("log is not enabled");
        }
    }
    public interface IBusConnection
    {
        public Task PublishAsync(byte[] data);
        public void printLog();//we added wait in this method implementation to allow disposal method to be executed before printing or copying the log. This is for test purpose
        public string copyLog();//we added wait in this method implementation to allow disposal method to be executed before printing or copying the log. This is for test purpose
        public Task wait();
    };
    //We should  lock the connection during the publish but this is out of our scope so for
    //simplicity I am going to skip this task.
    public class AWSConnection : IBusConnection
    {
        public AWSConnection()
        {
            _logger = new Logger();
        }
        private Logger _logger;
        public Task PublishAsync(byte[] data)
        {
            _logger.logMessage(Convert.ToBase64String(data));
            // Thread.Sleep(5);
            return Task.CompletedTask;
        }
        public void printLog()
        {
            Console.WriteLine("----AWS----");
            _logger.printLog();
        }
        public string copyLog()
        {
            return _logger.copyLog();
        }

        public Task wait()
        {
            Task.Delay(200);
            return Task.CompletedTask;
        }
    }
    public class MongoConnection : IBusConnection
    {
        private Logger _logger;
        public MongoConnection()
        {
            _logger = new Logger();
        }
        public Task PublishAsync(byte[] data)
        {
            _logger.logMessage(Convert.ToBase64String(data));
            Thread.Sleep(10);
            return Task.CompletedTask;
        }
        public void printLog()
        {
            Console.WriteLine("----Mongo----");
            _logger.printLog();
        }
        public string copyLog()
        {
            return _logger.copyLog();
        }
        public Task wait()
        {
            Thread.Sleep(2000);
            return Task.CompletedTask;
        }
    }
    public class AzureConnection : IBusConnection
    {
        public AzureConnection()
        {
            _logger = new Logger();
        }
        private Logger _logger;
        public Task PublishAsync(byte[] data)
        {
            _logger.logMessage(Convert.ToBase64String(data));
            Thread.Sleep(5);
            return Task.CompletedTask;
        }
        public void printLog()
        {
            Console.WriteLine("----Azure----");
            _logger.printLog();
        }
        public string copyLog()
        {
            return _logger.copyLog();
        }
        public Task wait()
        {
            Thread.Sleep(2000);
            return Task.CompletedTask;
        }
    }
}