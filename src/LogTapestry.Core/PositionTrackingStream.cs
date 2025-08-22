using System.Diagnostics;

namespace LogTapestry.Core
{
    /// <summary>
    /// A stream wrapper that tracks the total number of bytes read for position tracking.
    /// This is used to maintain precise stream position information for checkpointing.
    /// </summary>
    public class PositionTrackingStream : Stream
    {
        private readonly Stream _innerStream;
        private readonly long _initialPosition;
        private long _bytesRead;

        public PositionTrackingStream(Stream innerStream)
        {
            _innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));
            _initialPosition = innerStream.CanSeek ? innerStream.Position : 0;
            _bytesRead = 0;
        }

        /// <summary>
        /// Gets the total number of bytes read through this stream.
        /// </summary>
        public long TotalBytesRead => _bytesRead;

        /// <summary>
        /// Gets the current logical position (initial position + bytes read).
        /// </summary>
        public long CurrentPosition => _initialPosition + _bytesRead;

        public override bool CanRead => _innerStream.CanRead;
        public override bool CanSeek => _innerStream.CanSeek;
        public override bool CanWrite => _innerStream.CanWrite;
        public override long Length => _innerStream.Length;

        public override long Position
        {
            get => _innerStream.Position;
            set => _innerStream.Position = value;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesRead = _innerStream.Read(buffer, offset, count);
            _bytesRead += bytesRead;
            return bytesRead;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var bytesRead = await _innerStream.ReadAsync(buffer, offset, count, cancellationToken);
            _bytesRead += bytesRead;
            return bytesRead;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var bytesRead = await _innerStream.ReadAsync(buffer, cancellationToken);
            _bytesRead += bytesRead;
            return bytesRead;
        }

        public override int ReadByte()
        {
            var result = _innerStream.ReadByte();
            if (result != -1)
            {
                _bytesRead += 1;
            }
            return result;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _innerStream.Write(buffer, offset, count);
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await _innerStream.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await _innerStream.WriteAsync(buffer, cancellationToken);
        }

        public override void WriteByte(byte value)
        {
            _innerStream.WriteByte(value);
        }

        public override void Flush()
        {
            _innerStream.Flush();
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            await _innerStream.FlushAsync(cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _innerStream.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _innerStream.SetLength(value);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                // Don't dispose the inner stream - that's the caller's responsibility
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            // Don't dispose the inner stream - that's the caller's responsibility
            await base.DisposeAsync();
        }
    }
}