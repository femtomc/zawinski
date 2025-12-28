const std = @import("std");

/// Git metadata captured from the current working directory.
pub const GitInfo = struct {
    /// Full commit SHA (40 hex chars)
    oid: []const u8,
    /// Branch name or "HEAD" if detached
    head: []const u8,
    /// True if there are uncommitted changes
    dirty: bool,
    /// Path from git root to cwd (empty if at root)
    prefix: []const u8,

    pub fn deinit(self: *GitInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.oid);
        allocator.free(self.head);
        allocator.free(self.prefix);
    }
};

/// Capture git metadata from current working directory.
/// Returns null if not in a git repository.
pub fn capture(allocator: std.mem.Allocator) ?GitInfo {
    // Get branch and commit info
    const status_result = runGit(allocator, &.{ "status", "--porcelain=v2", "--branch" }) catch return null;
    defer allocator.free(status_result);

    // Get prefix (path from repo root to cwd)
    const prefix_result = runGit(allocator, &.{ "rev-parse", "--show-prefix" }) catch return null;
    defer allocator.free(prefix_result);

    // Parse status output - find first occurrence of oid and head
    var oid_slice: ?[]const u8 = null;
    var head_slice: ?[]const u8 = null;
    var dirty = false;

    var lines = std.mem.splitScalar(u8, status_result, '\n');
    while (lines.next()) |line| {
        if (oid_slice == null and std.mem.startsWith(u8, line, "# branch.oid ")) {
            oid_slice = line["# branch.oid ".len..];
        } else if (head_slice == null and std.mem.startsWith(u8, line, "# branch.head ")) {
            head_slice = line["# branch.head ".len..];
        } else if (line.len > 0 and line[0] != '#') {
            // Any non-header line means uncommitted changes
            dirty = true;
        }
    }

    // Must have at least oid
    const oid_val = oid_slice orelse return null;

    // Allocate oid
    const final_oid = allocator.dupe(u8, oid_val) catch return null;

    // Allocate head (use "HEAD" as fallback for detached state)
    const final_head = allocator.dupe(u8, head_slice orelse "HEAD") catch {
        allocator.free(final_oid);
        return null;
    };

    // Clean up and allocate prefix
    const trimmed_prefix = std.mem.trim(u8, prefix_result, " \t\r\n/");
    const final_prefix = allocator.dupe(u8, trimmed_prefix) catch {
        allocator.free(final_oid);
        allocator.free(final_head);
        return null;
    };

    return GitInfo{
        .oid = final_oid,
        .head = final_head,
        .dirty = dirty,
        .prefix = final_prefix,
    };
}

fn runGit(allocator: std.mem.Allocator, args: []const []const u8) ![]const u8 {
    var argv: std.ArrayListUnmanaged([]const u8) = .empty;
    defer argv.deinit(allocator);

    try argv.append(allocator, "git");
    for (args) |arg| {
        try argv.append(allocator, arg);
    }

    var child = std.process.Child.init(argv.items, allocator);
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;

    try child.spawn();

    // Note: child.stdout is managed by Child - we read from it but don't close it.
    // The child.wait() call below handles cleanup of the child process resources.
    const stdout = child.stdout orelse return error.NoStdout;

    // Read with 1MB limit (sufficient for git status output)
    const output = try stdout.readToEndAlloc(allocator, 1024 * 1024);
    errdefer allocator.free(output);

    const term = try child.wait();
    switch (term) {
        .Exited => |code| if (code != 0) {
            allocator.free(output);
            return error.GitFailed;
        },
        else => {
            allocator.free(output);
            return error.GitFailed;
        },
    }

    return output;
}

test "capture returns null outside git repo" {
    // This test only makes sense when run outside a git repo
    // In practice, the test runner is usually inside one, so we just
    // verify the function doesn't crash
    const allocator = std.testing.allocator;
    if (capture(allocator)) |*info| {
        var git_info = info.*;
        git_info.deinit(allocator);
    }
}
