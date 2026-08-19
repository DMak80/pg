#!/usr/bin/perl
# «Зависший подписчик» для проверки P4: открывает replication-соединение,
# запускает START_REPLICATION на слот и ЗАМЕРЗАЕТ — не читает поток и не шлёт
# подтверждений (keepalive-запросы walsender'а игнорируются). Слот остаётся
# активным с неподтверждённым confirmed_flush_lsn — ровно сценарий зависшего
# переезда из 12-bucket-pitfalls.md (P4).
use strict;
use warnings;
use IO::Socket::INET;

my ($slot, $pub) = @ARGV;
$slot //= 'stuck_move';
$pub  //= 'pub_p4';

my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => 5432, Proto => 'tcp')
    or die "connect: $!";

sub msg {    # (type, payload) → framed message
    my ($type, $payload) = @_;
    return $type . pack('N', length($payload) + 4) . $payload;
}

sub send_startup {
    my $params = "user\0postgres\0database\0postgres\0replication\0database\0\0";
    my $body   = pack('n2', 3, 0) . $params;    # protocol 3.0 = два uint16
    syswrite($sock, pack('N', length($body) + 4) . $body);
}

sub read_until_ready {    # читаем сообщения до ReadyForQuery ('Z')
    while (1) {
        sysread($sock, my $type, 1) or die "eof";
        sysread($sock, my $len, 4) or die "eof";
        my $n = unpack('N', $len) - 4;
        sysread($sock, my $payload, $n) if $n > 0;
        if ($type eq 'E') {    # ErrorResponse: печатаем и умираем
            my @f = split(/\0/, $payload);
            print "server error: @f\n";
            exit 1;
        }
        last if $type eq 'Z';
    }
}

send_startup();
read_until_ready();

my $sql = "START_REPLICATION SLOT $slot LOGICAL 0/0 (proto_version '1', publication_names '$pub')";
syswrite($sock, msg('Q', "$sql\0"));

# Дожидаемся CopyBothResponse ('W') — репликация началась
while (1) {
    sysread($sock, my $type, 1) or die "eof";
    sysread($sock, my $len, 4) or die "eof";
    my $n = unpack('N', $len) - 4;
    sysread($sock, my $payload, $n) if $n > 0;
    last if $type eq 'W';
}

print "slowconsumer: replication started, freezing (no feedback)\n";
# Замерзаем: не читаем, не подтверждаем. Walsender заблокируется на send,
# confirmed_flush_lsn встанет — слот начнёт удерживать WAL.
sleep 600;
