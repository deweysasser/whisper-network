//go:build linux && (amd64 || arm64)

package sandbox

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// runSeccomp interprets prog over a struct seccomp_data built from arch, nr
// and args, for the opcodes buildSeccomp emits, and returns the action.
func runSeccomp(t *testing.T, prog []unix.SockFilter, arch uint32, nr uintptr, args ...uint64) uint32 {
	t.Helper()
	data := make([]byte, 64)
	binary.LittleEndian.PutUint32(data[seccompOffNR:], uint32(nr))
	binary.LittleEndian.PutUint32(data[seccompOffArch:], arch)
	for i, a := range args {
		binary.LittleEndian.PutUint64(data[seccompOffArg0Low+8*i:], a)
	}
	var acc uint32
	for pc := 0; pc < len(prog); pc++ {
		ins := prog[pc]
		switch ins.Code {
		case bpfLoadW:
			require.LessOrEqual(t, int(ins.K)+4, len(data), "load out of range at %d", pc)
			acc = binary.LittleEndian.Uint32(data[ins.K:])
		case bpfAND:
			acc &= ins.K
		case bpfJEQ, bpfJGE, bpfJSET:
			var cond bool
			switch ins.Code {
			case bpfJEQ:
				cond = acc == ins.K
			case bpfJGE:
				cond = acc >= ins.K
			default:
				cond = acc&ins.K != 0
			}
			if cond {
				pc += int(ins.Jt)
			} else {
				pc += int(ins.Jf)
			}
		case bpfRet:
			return ins.K
		default:
			t.Fatalf("opcode %#x at %d not interpreted", ins.Code, pc)
		}
	}
	t.Fatal("program fell off the end")
	return 0
}

func TestSeccompFilterDecisions(t *testing.T) {
	allow := uint32(unix.SECCOMP_RET_ALLOW)
	eacces, eperm := seccompDeny(unix.EACCES), seccompDeny(unix.EPERM)
	const nb, cx = unix.SOCK_NONBLOCK, unix.SOCK_CLOEXEC
	type call struct {
		name string
		nr   uintptr
		args []uint64
		want uint32
	}
	sock := func(name string, domain, typ, proto uint64, want uint32) call {
		return call{name, unix.SYS_SOCKET, []uint64{domain, typ, proto}, want}
	}
	inetStream := []call{
		sock("AF_INET stream 0", unix.AF_INET, unix.SOCK_STREAM, 0, allow),
		sock("AF_INET6 stream tcp", unix.AF_INET6, unix.SOCK_STREAM, unix.IPPROTO_TCP, allow),
		sock("stream|nonblock tcp", unix.AF_INET, unix.SOCK_STREAM|nb, unix.IPPROTO_TCP, allow),
		sock("stream|nonblock|cloexec", unix.AF_INET6, unix.SOCK_STREAM|nb|cx, 0, allow),
		// The kernel reads the arguments as int: a garbage high word is
		// discarded there too, so the call is a plain AF_INET socket.
		sock("AF_INET with high-word garbage", 0xdeadbeef_00000000|unix.AF_INET, unix.SOCK_STREAM, 0, allow),
		sock("high-word garbage on AF_UNIX", 0x00000002_00000000|unix.AF_UNIX, unix.SOCK_STREAM, 0, eacces),
		sock("AF_INET6 dgram", unix.AF_INET6, unix.SOCK_DGRAM, 0, eacces),
		sock("AF_INET dgram udp", unix.AF_INET, unix.SOCK_DGRAM, unix.IPPROTO_UDP, eacces),
		sock("stream|0x10", unix.AF_INET, unix.SOCK_STREAM|0x10, 0, eacces),
		sock("raw", unix.AF_INET, unix.SOCK_RAW, unix.IPPROTO_ICMP, eacces),
		sock("seqpacket", unix.AF_INET, unix.SOCK_SEQPACKET, 0, eacces),
		sock("mptcp (262)", unix.AF_INET, unix.SOCK_STREAM, unix.IPPROTO_MPTCP, eacces),
		sock("sctp", unix.AF_INET, unix.SOCK_STREAM, unix.IPPROTO_SCTP, eacces),
		sock("AF_UNIX", unix.AF_UNIX, unix.SOCK_STREAM, 0, eacces),
		sock("AF_NETLINK", unix.AF_NETLINK, unix.SOCK_RAW, 0, eacces),
		sock("AF_PACKET", unix.AF_PACKET, unix.SOCK_RAW, 0, eacces),
		{"socketpair", unix.SYS_SOCKETPAIR, []uint64{unix.AF_UNIX, unix.SOCK_DGRAM, 0}, eacces},
		{"listen", unix.SYS_LISTEN, []uint64{3, 1}, eacces},
		{"accept", unix.SYS_ACCEPT, []uint64{3, 0, 0}, eacces},
		{"accept4", unix.SYS_ACCEPT4, []uint64{3, 0, 0, cx}, eacces},
		{"connect", unix.SYS_CONNECT, []uint64{3, 0, 16}, allow},
		{"sendto", unix.SYS_SENDTO, []uint64{3, 0, 5, unix.MSG_NOSIGNAL, 0, 0}, allow},
		{"sendto MSG_FASTOPEN", unix.SYS_SENDTO, []uint64{3, 0, 5, unix.MSG_FASTOPEN, 0, 16}, eacces},
		{"sendto MSG_FASTOPEN|MSG_NOSIGNAL", unix.SYS_SENDTO, []uint64{3, 0, 5, unix.MSG_FASTOPEN | unix.MSG_NOSIGNAL, 0, 16}, eacces},
		{"sendmsg", unix.SYS_SENDMSG, []uint64{3, 0, unix.MSG_NOSIGNAL}, allow},
		{"sendmsg MSG_FASTOPEN", unix.SYS_SENDMSG, []uint64{3, 0, unix.MSG_FASTOPEN}, eacces},
		{"sendmmsg MSG_FASTOPEN", unix.SYS_SENDMMSG, []uint64{3, 0, 1, unix.MSG_FASTOPEN}, eacces},
		{"io_uring_setup", unix.SYS_IO_URING_SETUP, []uint64{1, 0}, eperm},
		{"unshare CLONE_NEWNET", unix.SYS_UNSHARE, []uint64{unix.CLONE_NEWNET}, eperm},
		{"read", unix.SYS_READ, []uint64{0, 0, 1}, allow},
	}
	denyNet := []call{
		sock("AF_INET stream 0", unix.AF_INET, unix.SOCK_STREAM, 0, eacces),
		sock("AF_INET6 stream tcp", unix.AF_INET6, unix.SOCK_STREAM, unix.IPPROTO_TCP, eacces),
		{"socketpair", unix.SYS_SOCKETPAIR, []uint64{unix.AF_UNIX, unix.SOCK_STREAM, 0}, eacces},
		{"connect", unix.SYS_CONNECT, []uint64{3, 0, 16}, allow},
	}
	for name, c := range map[string]struct {
		rules seccompRules
		calls []call
	}{
		"inetStream":                 {seccompRules{denyNet: true, inetStream: true}, inetStream},
		"inetStream signal fallback": {seccompRules{denyNet: true, inetStream: true, signalPid: 4242}, inetStream},
		"denyNet":                    {seccompRules{denyNet: true}, denyNet},
	} {
		t.Run(name, func(t *testing.T) {
			prog, err := buildSeccomp(c.rules)
			require.NoError(t, err)
			require.Less(t, len(prog), 255, "jump offsets are 8 bits")
			for _, cl := range c.calls {
				got := runSeccomp(t, prog, seccompArch, cl.nr, cl.args...)
				assert.Equal(t, fmt.Sprintf("%#x", cl.want), fmt.Sprintf("%#x", got), cl.name)
			}
			assert.Equal(t, eperm, runSeccomp(t, prog, seccompArch^1, unix.SYS_READ), "foreign arch")
		})
	}
}
