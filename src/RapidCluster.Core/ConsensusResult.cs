using System.Diagnostics;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Base type for consensus round results.
/// Use pattern matching to handle the different outcomes.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal abstract record ConsensusResult
{
    private string DebuggerDisplay => GetType().Name;

    /// <summary>
    /// Consensus succeeded with a decided membership proposal.
    /// The proposal contains the complete new membership view state.
    /// </summary>
    /// <param name="Value">The decided membership proposal with all member info.</param>
    [DebuggerDisplay("{DebuggerDisplay,nq}")]
    public sealed record Decided(MembershipProposal Value) : ConsensusResult
    {
        private new string DebuggerDisplay => $"Decided(Members={Value.Members.Count}, Config={Value.ConfigurationId})";
    }

    /// <summary>
    /// Fast round failed due to vote split (multiple proposals, none reached threshold).
    /// Classic Paxos rounds should be attempted.
    /// </summary>
    public sealed record VoteSplit : ConsensusResult
    {
        /// <summary>Singleton instance.</summary>
        public static VoteSplit Instance { get; } = new();
        private VoteSplit() { }
    }

    /// <summary>
    /// Fast round failed due to too many delivery failures.
    /// Classic Paxos rounds should be attempted.
    /// </summary>
    public sealed record DeliveryFailure : ConsensusResult
    {
        /// <summary>Singleton instance.</summary>
        public static DeliveryFailure Instance { get; } = new();
        private DeliveryFailure() { }
    }

    /// <summary>
    /// Consensus round timed out without reaching a decision.
    /// Another round should be attempted.
    /// </summary>
    public sealed record Timeout : ConsensusResult
    {
        /// <summary>Singleton instance.</summary>
        public static Timeout Instance { get; } = new();
        private Timeout() { }
    }

    /// <summary>
    /// Consensus was cancelled (e.g., due to shutdown).
    /// </summary>
    public sealed record Cancelled : ConsensusResult
    {
        /// <summary>Singleton instance.</summary>
        public static Cancelled Instance { get; } = new();
        private Cancelled() { }
    }
}
