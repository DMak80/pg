#!/usr/bin/perl
# «Зависший подписчик» для проверки P4: открывает replication-соединение,
# запускает START_REPLICATION на слот и перестаёт ПОДТВЕРЖДАТЬ применение:
# поток читает (TCP жив, walsender не блокируется на send — иначе зависает
# и CHECKPOINT, ждущий освобождения слота для инвалидации), на keepalive
# отвечает feedback'ом со СТОЯЧЕЙ позицией (confirmed_flush не двигается).
# Слот остаётся активным с неподтверждённым confirmed_flush_lsn — ровно
# сценарий зависшего переезда из 12-bucket-pitfalls.md (P4): walsender жив,
# потребитель «читает, но не применяет» — лимит max_slot_wal_keep_size
# инвалидирует слот на ближайшем checkpoint.
use strict;
use warnings;
use IO::Socket::INET;
use IO::Select;
use Time::HiRes qw(time);

my ($slot, $pub, $standstill) = @ARGV;
$slot      //= 'stuck_move';
$pub       //= 'pub_p4';
$standstill //= '0/0';    # LSN, который «применён» — навсегда (0/0 = взять confirmed слота нельзя: узнаём ниже)

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

sub read_msg {    # → (type, payload) или undef на EOF
    return unless sysread($sock, my $type, 1);
    return unless sysread($sock, my $len, 4);
    my $n = unpack('N', $len) - 4;
    my $payload = '';
    while ($n > length($payload)) {    # дочитываем payload целиком
        my $got = sysread($sock, my $chunk, $n - length($payload));
        return unless $got;
        $payload .= $chunk;
    }
    return ($type, $payload);
}

sub lsn_to_int {
    my ($hi, $lo) = ($_[0] =~ /^([0-9A-F]+)\/([0-9A-F]+)$/i) or die "bad lsn $_[0]";
    return hex($hi) * 2**32 + hex($lo);
}

sub send_feedback {    # подтверждаем применение ТОЛЬКО стоячей позиции
    my ($ln) = @_;
    my $ts = int(time * 1_000_000);
    syswrite($sock, msg('d', 'r' . pack('Q3 q C', $ln, $ln, $ln, $ts, 0)));
}

send_startup();
while (1) {
    my ($type, $payload) = read_msg() or die "eof";
    if ($type eq 'E') { my @f = split(/\0/, $payload); die "server error: @f\n"; }
    last if $type eq 'Z';
}

my $sql = "START_REPLICATION SLOT $slot LOGICAL 0/0 (proto_version '1', publication_names '$pub')";
syswrite($sock, msg('Q', "$sql\0"));

# Дожидаемся CopyBothResponse ('W') и берём стартовую позицию из первого
# XLogData/Keepalive — её и «подтверждаем» навсегда (применение заморожено)
my $frozen;
while (1) {
    my ($type, $payload) = read_msg() or die "eof";
    die "server error\n" if $type eq 'E';
    next if $type ne 'd';
    my $code = substr($payload, 0, 1);
    if    ($code eq 'w') { $frozen = unpack('Q', substr($payload, 1, 8)) unless defined $frozen; }
    elsif ($code eq 'k') { $frozen = unpack('Q', substr($payload, 1, 8)) unless defined $frozen; }
    last if defined $frozen;
}
# стартовая позиция не лучше подтверждённой при создании слота
my $floor = lsn_to_int($standstill);
$frozen = $floor if defined($floor) && $floor > 0 && $frozen > $floor;
print "slowconsumer: replication started, reading stream, feedback frozen at lsn $frozen\n";

# Читаем всё (drain); на keepalive с request-reply отвечаем стоячим feedback'ом,
# а при простое потока шлём его сами каждые 10с — иначе wal_sender_timeout
# (60с без контакта от клиента) порвёт соединение и деактивирует слот
my $sel = IO::Select->new($sock);
while (1) {
    if ($sel->can_read(10)) {
        my ($type, $payload) = read_msg() or last;
        next if $type ne 'd';
        my $code = substr($payload, 0, 1);
        if ($code eq 'k' && ord(substr($payload, -1)) & 1) {
            send_feedback($frozen);
        }
    } else {
        send_feedback($frozen);
    }
}
print "slowconsumer: connection closed\n";
