using RapidCluster.Exceptions;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for Rapid exception classes.
/// </summary>
public class ExceptionTests
{
    [Fact]
    public void JoinExceptionDefaultConstructorWorks()
    {
        var ex = new JoinException();
        Assert.NotNull(ex);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void JoinExceptionWithMessageWorks()
    {
        var ex = new JoinException("Test message");
        Assert.Equal("Test message", ex.Message);
    }

    [Fact]
    public void JoinExceptionWithMessageAndInnerExceptionWorks()
    {
        var inner = new InvalidOperationException("Inner");
        var ex = new JoinException("Outer", inner);

        Assert.Equal("Outer", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionDefaultConstructorWorks()
    {
        var ex = new NodeAlreadyInRingException();
        Assert.NotNull(ex);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionWithMessageWorks()
    {
        var ex = new NodeAlreadyInRingException("Custom message");
        Assert.Equal("Custom message", ex.Message);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionWithEndpointWorks()
    {
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        var ex = new NodeAlreadyInRingException(endpoint);

        // Hostname may be base64 encoded, check port instead
        Assert.Contains("1234", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionWithMessageAndInnerExceptionWorks()
    {
        var inner = new InvalidOperationException("Inner");
        var ex = new NodeAlreadyInRingException("Outer", inner);

        Assert.Equal("Outer", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionNullEndpointThrowsArgumentNull() => Assert.Throws<ArgumentNullException>(() => new NodeAlreadyInRingException((Endpoint)null!));

    [Fact]
    public void NodeNotInRingExceptionDefaultConstructorWorks()
    {
        var ex = new NodeNotInRingException();
        Assert.NotNull(ex);
    }

    [Fact]
    public void NodeNotInRingExceptionWithMessageWorks()
    {
        var ex = new NodeNotInRingException("Custom message");
        Assert.Equal("Custom message", ex.Message);
    }

    [Fact]
    public void NodeNotInRingExceptionWithEndpointWorks()
    {
        var endpoint = Utils.HostFromParts("192.168.1.1", 5000);
        var ex = new NodeNotInRingException(endpoint);

        // Hostname may be base64 encoded, check port instead
        Assert.Contains("5000", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NodeNotInRingExceptionWithMessageAndInnerExceptionWorks()
    {
        var inner = new InvalidOperationException("Inner");
        var ex = new NodeNotInRingException("Outer", inner);

        Assert.Equal("Outer", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void NodeNotInRingExceptionNullEndpointThrowsArgumentNull() => Assert.Throws<ArgumentNullException>(() => new NodeNotInRingException((Endpoint)null!));

    [Fact]
    public void JoinExceptionInheritsFromException()
    {
        var ex = new JoinException("test");
        Assert.IsType<Exception>(ex, exactMatch: false);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionInheritsFromException()
    {
        var ex = new NodeAlreadyInRingException("test");
        Assert.IsType<Exception>(ex, exactMatch: false);
    }

    [Fact]
    public void NodeNotInRingExceptionInheritsFromException()
    {
        var ex = new NodeNotInRingException("test");
        Assert.IsType<Exception>(ex, exactMatch: false);
    }

    [Fact]
    public void JoinExceptionCanBeThrownAndCaught()
    {
        bool thrown;
        try
        {
            throw new JoinException("Test");
        }
        catch (JoinException)
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    public void NodeAlreadyInRingExceptionCanBeThrownAndCaught()
    {
        bool thrown;
        try
        {
            throw new NodeAlreadyInRingException("Test");
        }
        catch (NodeAlreadyInRingException)
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    public void NodeNotInRingExceptionCanBeThrownAndCaught()
    {
        bool thrown;
        try
        {
            throw new NodeNotInRingException("Test");
        }
        catch (NodeNotInRingException)
        {
            thrown = true;
        }
        Assert.True(thrown);
    }
}
