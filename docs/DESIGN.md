# Catatan desain

README adalah tur singkatnya. Ini bagian dalamnya: invariant yang dijaga setiap
package, kenapa bentuk yang sekarang dipilih alih-alih alternatif yang kelihatan
jelas, dan di mana desainnya sengaja mengorbankan sesuatu.

Baca README dulu. Tidak ada apa pun di sini yang diperlukan untuk memakai library
ini.

---

## Daftar isi

1. [Masalah inti](#1-masalah-inti)
2. [`queue`: kepemilikan dan waktu](#2-queue-kepemilikan-dan-waktu)
3. [`worker`: dua context, satu shutdown](#3-worker-dua-context-satu-shutdown)
4. [`ratelimit`: kebenaran saat penolakan](#4-ratelimit-kebenaran-saat-penolakan)
5. [`eventbus`: pencocokan dan backpressure](#5-eventbus-pencocokan-dan-backpressure)
6. [Taksonomi kegagalan](#6-taksonomi-kegagalan)
7. [Bug yang ditemukan saat proses verifikasi](#7-bug-yang-ditemukan-saat-proses-verifikasi)
8. [Apa yang akan berubah untuk backend terdistribusi sungguhan](#8-apa-yang-akan-berubah-untuk-backend-terdistribusi-sungguhan)
9. [Strategi testing](#9-strategi-testing)

---

## 1. Masalah inti

Job queue adalah state machine atas waktu, dan setiap bug sulit di dalamnya
berasal dari dua pertanyaan yang kelihatan mudah:

1. **Siapa yang memiliki sebuah job saat ini?** Begitu sebuah job diserahkan ke
   worker, queue kehilangan kemampuan mengamatinya. Jika worker-nya mati, queue
   harus pada akhirnya menyadarinya — tetapi ia tidak bisa membedakan "masih
   bekerja" dari "sudah mati" tanpa sebuah lease.
2. **Apa yang terjadi saat waktu berlalu?** Lease yang kedaluwarsa dan retry
   terjadwal yang menjadi jatuh tempo adalah jenis event yang sama: sebuah deadline
   di masa depan yang mengubah status sebuah job. Keduanya tidak boleh ditangani
   oleh dua mekanisme berbeda, atau urutan di antara keduanya menjadi tak
   teramati.

`dispatch` menjawab (1) dengan **visibility timeout** — sebuah delivery adalah
lease dengan deadline, dan queue mempersenjatai ulang job tersebut ketika
deadline-nya lewat — dan (2) dengan **satu goroutine scheduler** yang memiliki
setiap transisi yang digerakkan waktu. Kedua keputusan itu merambat ke sebagian
besar desain sisanya.

Bentuk alternatifnya, yang sengaja dihindari codebase ini, adalah goroutine latar
belakang per job atau per reservation. Ia lebih mudah ditulis dan jauh lebih sulit
dicerna: jumlah goroutine menjadi sebanding dengan pekerjaan yang sedang melayang,
shutdown menjadi fan-in atas himpunan tak berbatas, dan dua timer yang berlomba
me-requeue job yang sama menghasilkan delivery duplikat. Satu scheduler membuat
jumlah goroutine menjadi konstanta dan urutannya total.

---

## 2. `queue`: kepemilikan dan waktu

### 2.1 Scheduler adalah satu-satunya jam

`queue.New` menjalankan tepat satu goroutine scheduler. Ia satu-satunya penulis
delayed-delivery dan visibility state. Producer dan consumer mengambil mutex untuk
menyentuh heap, tetapi *waktu* hanya milik scheduler.

Scheduler menghitung deadline tertunda paling awal (`RunAt` job tertunda
berikutnya, kedaluwarsa reservation berikutnya) lalu tidur sampai saat itu. Ketika
ia bangun, ia menguras semua yang sudah jatuh tempo. Bangun terlambat itu tidak
berbahaya — kesiapan ditentukan dengan membandingkan timestamp, bukan dengan
mempercayai wakeup-nya — jadi tidak ada ketergantungan kebenaran pada akurasi
timer. `WithPollInterval` ada sebagai batas bawah berapa lama scheduler akan
tidur, yang membatasi latency untuk test yang menginginkan turnaround cepat.

**Kenapa ini penting:** karena satu goroutine menerapkan setiap transisi yang
digerakkan waktu, tidak ada interleaving di mana dua timer berlomba me-requeue job
yang sama. Delivery duplikat lalu menjadi properti dari *protokol lease* semata,
yang merupakan permukaan yang jauh lebih kecil untuk di-test.

### 2.2 Reservation, bukan penghapusan

`Dequeue` tidak menghapus sebuah job. Ia mencatat reservation dengan sebuah
deadline lalu mengembalikan `Delivery`:

```
Delivery{ Job: Entry, Attempt: int, Deadline: time.Time, token }
```

`Ack` memensiunkan job. `Nack` entah menjadwalkan retry atau melakukan
dead-letter. `Extend` mendorong deadline-nya lebih jauh. Jika tidak ada dari
semuanya yang datang sebelum `Deadline`, scheduler mengembalikan job ke ready set
— **tanpa menghabiskan satu attempt**.

Klausa terakhir itu adalah pilihan yang disengaja. Reservation yang kedaluwarsa
berarti worker tidak pernah melapor kembali: bisa jadi ia crash, bisa jadi ia
macet, atau mesinnya mungkin di-suspend. Menagih satu attempt untuk vonis yang
tidak pernah disampaikan akan membiarkan worker yang kurang beruntung menghabiskan
seluruh anggaran retry sebuah job tanpa handler-nya pernah selesai berjalan. Jadi
reservation yang kedaluwarsa itu gratis, dan hanya `Nack` eksplisit yang
menghabiskan satu attempt.

Biayanya: job yang handler-nya secara konsisten hidup lebih lama dari visibility
timeout-nya akan dikirim ulang selamanya. Itu patologi nyata, dan perbaikan yang
jujur adalah attempt counter berbatas yang menghitung *delivery* sekaligus vonis.
Ia tidak diimplementasikan, karena semantiknya cepat membingungkan (apa arti
pengiriman ulang job dengan `MaxAttempts: 1`?) dan perilaku saat ini setidaknya
bisa dipertahankan: **sebuah job hanya dihukum karena vonis yang ia hasilkan.**

### 2.3 Sentinel zero-time

Job yang sifatnya immediate dinormalkan ke `RunAt` bernilai nol, bukan
`time.Now()`.

Ini kelihatan seperti micro-optimization dan sebenarnya adalah perbaikan
kebenaran. Ketika setiap entry membawa `time.Now()` miliknya sendiri, ordering
ready heap `(priority, readyAt, seq)` ditentukan oleh timestamp untuk setiap
pasangan berprioritas sama, dan `seq` — counter enqueue monotonik yang ada justru
untuk memecah seri — hanya pernah menyala ketika dua timestamp identik bit-per-bit.
`TestPriorityOrdering` menangkap ini. Menormalkan ke sentinel membuat `seq` menjadi
tiebreaker yang sesungguhnya, sehingga urutan FIFO di dalam satu kelas prioritas
dijamin alih-alih bersifat probabilistik.

### 2.4 Backoff memberi cap setelah jitter

```go
delay := base * (1 << (attempt - 1))   // exponential
delay += jitter(delay)                 // full jitter
if delay > max { delay = max }         // cap LAST
```

Cap diterapkan setelah jitter, bukan sebelumnya. Menerapkannya lebih dulu membuat
jitter bisa mendorong hasilnya melewati plafon, sehingga maksimum yang
didokumentasikan terlampaui justru pada attempt-attempt akhir di mana itu paling
penting. Bug nyata kedua yang ditemukan test. Full jitter (uniform pada
`[0, delay)`) alih-alih decorrelated jitter adalah pilihan kesederhanaan: pada
plafon 30 detik dengan segelintir retry, argumen thundering-herd untuk varian yang
lebih canggih tidak menggigit.

### 2.5 Satu mutex global, dengan sengaja

Queue memakai satu `sync.Mutex` untuk seluruh state. Contention bukan bottleneck
pada skala yang jadi sasaran desain ini, dan alternatifnya — sharded lock atau
struktur lock-free — melipatgandakan jumlah ordering yang harus diverifikasi
seorang reviewer. Ketika backend-nya ditukar dengan store sungguhan, mutex ini
hilang sepenuhnya dan digantikan oleh concurrency control milik store tersebut,
jadi mengoptimalkannya sekarang berarti mengoptimalkan kode yang sudah dijadwalkan
untuk dihapus.

---

## 3. `worker`: dua context, satu shutdown

### 3.1 Pemisahannya

`Start(ctx)` menurunkan sebuah context *dispatch* tetapi meneruskan `ctx` asli
milik pemanggil ke handler. Ini keputusan paling berkonsekuensi di package ini.

- **Context dispatch** menggerbangi `Dequeue` dan penantian limiter.
  Membatalkannya langsung melepaskan dispatcher yang sedang terparkir pada queue
  kosong, alih-alih membuat shutdown menunggu poll interval atau deadline milik
  pemanggil.
- **Context handler** adalah context milik pemanggil, tak tersentuh. `Shutdown`
  yang graceful **tidak** membatalkannya, sehingga job yang sedang melayang bisa
  selesai.

Mencampur keduanya menghasilkan salah satu dari dua bug tergantung ke arah mana
pembatalannya merambat: entah shutdown kembali selagi handler masih mengubah
state (pekerjaan hilang), atau shutdown terblokir sampai setiap handler selesai
tak peduli berapa lama (drain tak berbatas). Memisahkannya membuat pool bisa
berhenti *menarik* pekerjaan sementara pekerjaan yang melayang tetap berjalan
sampai selesai.

Pemisahan itu hanya berguna kalau pemanggilnya ikut menjaganya. `Start(ctx)`
memakai `ctx` apa adanya sebagai context handler, jadi menyerahkan context
`SIGTERM` ke `Start` justru membuat `Shutdown` membatalkan handler yang sedang
ia tunggu — persis bug yang pemisahan ini dimaksudkan untuk mencegah. Karena itu
`cmd/dispatchd` tidak menyerahkan context signal ke pool: `serve` menurunkan
context handler dari context signal lewat `context.WithoutCancel`, sehingga
handler tetap hidup saat drain sementara nilainya tetap terbawa.

`Shutdown` bersifat idempotent dan dibatasi oleh `WithDrainTimeout`. Saat
kedaluwarsa ia mengembalikan error yang membungkus `context.DeadlineExceeded`, dan
job yang ditinggalkan jatuh kembali ke visibility timeout milik queue. Itulah
hasil yang jujur; alternatifnya adalah penantian tak berbatas.

### 3.2 `applyNack` adalah satu-satunya penafsir jawaban `Nack`

`Nack` yang mengembalikan `ErrRetryScheduled` berarti **retry-nya sudah
dijadwalkan** — sebuah sukses. Ada dua call site yang memanggil nack: jalur
kegagalan biasa dan jalur yang terinterupsi shutdown. Ketika masing-masing
menafsirkan hasilnya secara independen, jalur shutdown mencatat `ErrRetryScheduled`
sebagai `nack during shutdown failed`, membuat setiap graceful shutdown yang sehat
terlihat seperti kehilangan data tepat pada saat operator sedang mengamati.

`applyNack` sekarang memusatkan penafsirannya: `ErrRetryScheduled` →
`RetryScheduled` + counter `retried` + log debug; `ErrJobFailed` → `DeadLettered`
+ counter `dead` + log error; `ErrUnknownJob` → peringatan bahwa reservation-nya
kedaluwarsa; apa pun selainnya → error. Kedua jalur memanggilnya, sehingga keduanya
tidak bisa menyimpang.

### 3.3 Akuntansi kegagalan, dinyatakan secara sempit

Sebuah job dihitung sebagai **berhasil** hanya ketika handler mengembalikan nil
*dan* `Ack`-nya diterima. `Ack` yang gagal berarti reservation-nya kedaluwarsa dan
job-nya akan dikirim ulang; menghitungnya sebagai sukses akan melebih-lebihkan
throughput justru pada run-run di mana worker-nya terlalu lambat. `Processed`
menghitung vonis, bukan delivery, jadi job yang dikirim ulang dihitung dua kali —
itulah sebabnya `Succeeded`, `Failed`, `Retried`, dan `Dead` diekspos terpisah
alih-alih digabung.

### 3.4 Panic ditahan

`safeHandle` mengubah panic di handler menjadi `ErrHandlerPanic` yang membungkus
job ID, sehingga job-nya di-nack dan di-retry seperti kegagalan lain.
`safeNotify` memperluas penahanan yang sama ke `OnResult`. Kontrak pool-nya adalah
bertahan terhadap handler yang panic: kehilangan goroutine-nya akan mengurangi
kapasitas secara senyap dan permanen.

`New` panic pada `Sink` atau handler yang nil. Handler nil yang diam-diam diganti
dengan no-op adalah default terburuk yang mungkin — setiap job "berhasil" sambil
tidak melakukan apa pun — dan itu kesalahan pemrograman, jadi ia gagal pada waktu
konstruksi.

---

## 4. `ratelimit`: kebenaran saat penolakan

### 4.1 Pengisian ulang lazy, tanpa goroutine

Token bucket mengisi ulang secara aritmetik:

```go
elapsed := now.Sub(b.last)
b.tokens = min(b.burst, b.tokens + elapsed.Seconds()*b.rate)
```

Tanpa goroutine latar belakang, tanpa timer per limiter. Biayanya O(1) per
panggilan dan sebanding dengan jumlah limiter, bukan dengan token atau waktu wall
clock. Proses yang memegang sepuluh ribu limiter yang mayoritas menganggur tidak
membayar apa pun untuknya, yang akan salah untuk desain ticker-per-bucket. Waktu
berasal dari `WithClock`, sehingga setiap pengisian ulang deterministik di test;
tidak ada yang tidur kecuali `Wait`.

### 4.2 `Wait` memeriksa context sebelum menghabiskan

`Wait` pada context yang sudah dibatalkan mengembalikan `ctx.Err()` **tanpa
menghabiskan token**. Ini bug fail-open yang ditemukan saat proses verifikasi:
loop aslinya me-reserve dulu dan memeriksa context belakangan, sehingga pemanggil
yang dibatalkan menghabiskan kapasitas yang tidak akan pernah ia pakai, diam-diam
mengecilkan rate efektif bagi semua orang lain saat beban tinggi.

Memperbaikinya memunculkan bug kedua — `d := l.Reserve()` yang menaungi
reservation luarnya, membuat loop me-reserve ulang di setiap putaran. Keduanya
dikunci oleh test, karena keduanya hanya muncul saat contention.

### 4.3 `Keyed` bukan `Limiter`, dengan sengaja

`TokenBucket`, `FixedWindow`, dan `Multi` mengimplementasikan `Limiter` — tanpa
key, satu anggaran. `Keyed` memerlukan key, jadi method-nya adalah `Allow(key)`,
`Reserve(key)`, dan `Wait(ctx, key)`, dan ia **tidak** mengimplementasikan
`Limiter`.

Jika ia mengimplementasikannya, panggilan tanpa key harus gagal secara fail-open
(izinkan semuanya) atau fail-closed (tolak semuanya). Keduanya adalah bencana
senyap bagi sebuah rate limiter, jadi API-nya justru membuat panggilan tanpa key
menjadi error **compile**. `TestKeyedIsNotALimiter` menegaskan relasi tipe yang
menjaga hal ini tetap benar.

`Keyed` membatasi ruang key-nya (`maxKeys`) dengan eviction, sehingga penyerang
yang memasok key-key baru tidak bisa menumbuhkan memori tanpa batas. Itu trade-off
nyata: meng-evict sebuah key me-reset anggarannya, jadi penyerang yang bisa
merotasi key *dan* memicu eviction mendapat bypass parsial. Sistem produksi akan
melakukan evict berdasarkan LRU dengan masa tinggal minimum. Didokumentasikan,
bukan disembunyikan.

### 4.4 Argumen tidak valid di-clamp

Rate negatif, burst nol, window yang tidak positif — masing-masing di-clamp ke
minimum yang wajar alih-alih panic. Konfigurasi yang datang dari flag atau
environment seharusnya tidak bisa membuat prosesnya crash pada waktu konstruksi.
`Multi` memperkuat ini: `Reserve` mengembalikan penantian **maksimum** dari
anak-anaknya, karena setiap anak harus terpenuhi.

---

## 5. `eventbus`: pencocokan dan backpressure

### 5.1 Semantik pencocokan

Pattern dipisahkan titik dengan `*` (tepat satu segmen) dan `>` (satu atau lebih
segmen tersisa, sah hanya sebagai segmen terakhir). `job.>` cocok dengan
`job.created` tetapi tidak dengan topik telanjang `job` — "satu atau lebih"
alih-alih "nol atau lebih", karena kecocokan topik telanjang hampir tidak pernah
yang dimaksud penulisnya dan berlangganan lebih banyak dari yang diniatkan secara
senyap itu lebih buruk daripada tidak mencocokkan apa pun.

### 5.2 `Publish` sinkron; handler-nya tidak

`Publish` mencocokkan subscription dan mengirim ke buffered channel milik setiap
subscriber *sebelum kembali*. Eksekusi handler terjadi pada goroutine worker milik
subscriber itu sendiri. `PublishSync` menunggu handler, untuk pemanggil yang
pernyataan berikutnya bergantung pada event yang sudah teramati.

`matching()` mengambil snapshot himpunan subscriber di bawah lock bus lalu
**melepasnya sebelum mengirim**. Di bawah `DropPolicy: Block` sebuah pengiriman
channel bisa menunggu tanpa batas, dan memegang lock bus sepanjang itu akan
membuat satu subscriber lambat menahan setiap publisher di proses tersebut.
Snapshot-nya aman karena penghapusan dikoordinasikan lewat lock milik subscriber
itu sendiri.

### 5.3 Masalah send-on-closed-channel

Footgun klasik Go, diselesaikan dengan tiga potong state:

```go
sendMu  sync.RWMutex
closed  bool
closing chan struct{}
```

`Subscription.Close` menutup `s.closing`, menghapus subscription dari bus, lalu
mengambil `sendMu.Lock()` dan menutup `s.ch` tepat sekali. Publisher memegang
`sendMu.RLock()` sepanjang pengirimannya, sehingga ia entah selesai sebelum
closer-nya mengambil write lock atau mengamati `closing` sudah tertutup lalu
menyerah. Tidak ada pengiriman yang pernah berlomba dengan close.

`Close` pada bus bersifat **drain-then-stop**: ia menutup subscription,
membiarkan event yang ter-buffer mencapai handler sebelum worker-nya keluar.
`Subscribe` pada bus yang sudah tertutup mengembalikan subscription yang sudah
tertutup plus `ErrBusClosed`, sehingga tidak ada pemanggil yang akhirnya memegang
handle yang kelihatan hidup pada bus yang mati.

### 5.4 `DropNewest` adalah default-nya, dan itulah intinya

Drop policy default membuang event yang **masuk** ketika buffer subscriber penuh,
alih-alih memblokir publisher. Ini kebalikan dari apa yang biasanya tersirat oleh
"reliable bus", dan memang disengaja: bus adalah infrastruktur observasional yang
berada di samping queue yang sudah menyediakan jalur durable dan bisa di-retry.
Memblokir producer karena subscriber logging adalah kegagalan yang lebih buruk.
`WithDropPolicy(Block)` tersedia di tempat backpressure diinginkan, dan `Dropped()`
memunculkan hitungannya dalam kedua kasus.

**Aturan yang mengikutinya:** jika sebuah event tidak boleh hilang, tempatnya di
queue, bukan di bus.

---

## 6. Taksonomi kegagalan

Setiap package membedakan "pemanggil melakukan kesalahan" dari "dunia luar
melakukan kesalahan", karena responsnya berbeda.

| Sentinel | Kelas | Arti |
| --- | --- | --- |
| `queue.ErrClosed` | pemanggil | Queue sudah ditutup; berhenti memanggilnya |
| `queue.ErrInvalidEntry` | pemanggil | ID hilang atau field tidak valid |
| `queue.ErrUnknownJob` | dunia luar | Reservation kedaluwarsa; job-nya sudah bergerak |
| `queue.ErrRetryScheduled` | **sukses** | `Nack` menjadwalkan sebuah retry |
| `queue.ErrJobFailed` | terminal | Attempt habis; sudah dead-letter |
| `worker.ErrAlreadyStarted` | pemanggil | `Start` dipanggil dua kali |
| `worker.ErrPoolClosed` | pemanggil | `Start` setelah `Shutdown` |
| `worker.ErrNilContext` | pemanggil | `Start(nil)` |
| `worker.ErrHandlerPanic` | dunia luar | Handler panic; job di-nack |
| `eventbus.ErrBusClosed` | pemanggil | Bus tertutup |
| `eventbus.ErrInvalidPattern` | pemanggil | Pattern topik tidak valid |
| `eventbus.ErrSubClosed` | pemanggil | Subscription tertutup |

`ErrRetryScheduled` sebagai nilai sukses adalah yang paling menggigit. Kode apa
pun yang memperlakukan kembalian `Nack` yang non-nil sebagai kegagalan itu salah,
dan §3.2 ada karena kesalahan itu sudah pernah terjadi sekali.

Error dibungkus dengan `%w` agar `errors.Is` bekerja menembus lapisan-lapisan, dan
error drain saat shutdown membungkus `context.DeadlineExceeded` agar pemanggil
bisa memperlakukannya seragam dengan deadline lain mana pun.

---

## 7. Bug yang ditemukan saat proses verifikasi

Dicatat karena inilah bukti bahwa test-nya membayar dirinya sendiri. Setiap
satunya ditemukan oleh test, bukan dengan membaca.

| Bug | Gejala | Perbaikan |
| --- | --- | --- |
| Urutan prioritas tidak bekerja | `seq` tidak pernah memecah seri; urutannya bergantung resolusi jam | Sentinel zero-time untuk job immediate |
| Plafon backoff terlampaui | Cap diterapkan sebelum jitter, sehingga retry akhir melampaui maksimum | Cap setelah jitter |
| `Wait` fail-open | Pemanggil yang dibatalkan tetap menghabiskan satu token | Periksa `ctx.Err()` sebelum dan sesudah `Allow` |
| Reservation yang tertutupi | Loop me-reserve ulang setiap iterasi setelah perbaikan di atas | Angkat `d := l.Reserve()` keluar dari loop |
| Kontradiksi API `Keyed` | Dokumentasi menyebut keyed, stub-nya tanpa key dan fail open | Signature keyed; `Limiter` sengaja tidak diimplementasikan |
| Log shutdown menyesatkan | Shutdown yang sehat mencatat `nack during shutdown failed` | `applyNack` dipakai bersama kedua jalur |
| Struct pembungkus `job` | Struct satu field dengan `release func()` yang mati | Diringkas menjadi `chan queue.Delivery` |
| Tabrakan ID benchmark | `time.Now().UnixNano()` menghasilkan ID hidup yang duplikat | Counter `atomic.Uint64` |
| Benchmark hang | Mengisi awal `1<<16` tetap sementara `RunParallel` menotal `b.N` | Isi awal tepat `b.N` |
| Benchmark hang (2) | Clock yang beku membuat `Wait` memblokir begitu burst-nya habis | `benchClock.step` memajukan waktu di setiap pembacaan |
| Shutdown tidak men-drain | `SIGTERM` membatalkan handler yang sedang melayang; job kembali sebagai `handler interrupted by shutdown` | `serve` menurunkan context handler dari context signal lewat `context.WithoutCancel` |
| Handler dipanggil dengan delivery kosong | `Dequeue` yang gagal jatuh ke jalur sukses dan menjalankan handler atas `Delivery` bernilai nol | Tiap cabang error terminal (`return`), error transien `continue` |

Salah satu bug benchmark di atas layak digeneralisasi: **sebuah benchmark tidak
boleh memakai clock yang beku di tempat kode yang diuji bisa memblokir.**
`BenchmarkWaitImmediate` lulus pada `-benchtime 50ms` (b.N ≈ burst, jadi ia tidak
pernah harus menunggu) dan menggantung seluruh run `-bench .` pada `100ms`. Clock
yang beku hanya aman untuk jalur penolakan yang tidak pernah memblokir.

---

## 8. Apa yang akan berubah untuk backend terdistribusi sungguhan

Interface-interface ini dibentuk dengan bertanya apa yang akan dibutuhkan backend
SQS/Redis/Postgres. Secara konkret:

- **`queue.Backend`** di balik permukaan yang ada sekarang. Redis dipetakan ke
  sorted set untuk job tertunda plus lock per job untuk reservation; Postgres
  dipetakan ke `FOR UPDATE SKIP LOCKED` plus `LISTEN/NOTIFY` untuk wakeup.
  Keduanya mengubah scheduler dari "memiliki jam" menjadi "bertanya ke store apa
  yang sudah jatuh tempo", itulah sebabnya scheduler-nya diisolasi.
- **Fencing token.** `Delivery.token` adalah `uint64` in-process. Antar proses ia
  harus menjadi fencing token monotonik agar worker yang bangkit kembali tidak
  bisa `Ack` sebuah job yang kini dimiliki worker lain. Inilah perubahan yang
  paling memengaruhi API publik: `Ack` harus gagal dengan berisik pada token yang
  basi alih-alih mengembalikan `ErrUnknownJob`.
- **Idempotency key.** Pengiriman at-least-once adalah kontrak yang harus
  dihormati oleh *consumer*-nya. Queue bisa membantu dengan memunculkan dedup key,
  tetapi ia tidak bisa menyediakan exactly-once sendirian.
- **Attempt berbatas untuk reservation yang kedaluwarsa.** Aturan "reservation
  kedaluwarsa itu gratis" di §2.2 memerlukan counter pendamping, atau job yang
  patologis menjadi abadi.
- **Fair scheduling.** Satu priority heap global membuat pekerjaan berprioritas
  rendah kelaparan di bawah beban yang terus-menerus. Queue per-tenant dengan
  weighted round-robin adalah perbaikan standarnya.

---

## 9. Strategi testing

Kira-kira setengah repo ini adalah test, dan strateginya disengaja:

- **Waktu di-inject.** Setiap package yang bergantung pada waktu wall clock
  menerima sebuah clock, sehingga backoff, pengisian ulang, dan kedaluwarsa
  visibility ditegaskan secara tepat alih-alih ditunggu dengan tidur.
- **Timer sungguhan hanya untuk penantian yang memblokir**, dengan margin yang
  lega. `Wait` dan drain memang benar-benar memblokir, jadi test-test itu memakai
  deadline yang cukup besar agar jitter scheduling tidak membuatnya flaky.
- **Setiap package punya test kebocoran goroutine.** `make leak` menjalankannya.
  Setelah objek yang diuji ditutup, jumlah goroutine harus kembali ke baseline —
  satu-satunya cara menangkap scheduler atau worker yang dijalankan lalu tidak
  pernah di-join.
- **Idempotensi ditegaskan, bukan diasumsikan.** `Close`/`Shutdown` dipanggil
  berulang kali dan setelah jalur error, karena "ia idempotent" luruh begitu
  seseorang menambahkan sebuah field.
- **Tanpa test framework.** Kosakata assertion-nya adalah `t.Fatalf` dengan pesan
  yang menyebutkan ekspektasi dan nilai yang teramati. Helper yang menyembunyikan
  itu adalah beban.
- **Test yang mengkodekan sebuah bug diberi komentar yang menyatakannya.** Test
  regresi untuk sentinel prioritas, cap backoff, dan nack saat shutdown
  masing-masing menjelaskan apa yang dulunya salah, sehingga pembaca di masa depan
  tidak bisa "menyederhanakan" perbaikannya hingga hilang.

Benchmark diukur, tidak pernah diperkirakan, dan berada di samping kode yang
diujinya sehingga tidak bisa membusuk secara senyap. Setiap angka di README berasal
dari run yang tercatat.
