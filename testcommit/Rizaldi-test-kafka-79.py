import avro.schema
import io
import datetime
import random
import calendar

from avro.datafile import DataFileWriter
from avro.io import DatumWriter, BinaryEncoder
from io import BytesIO
from kafka import KafkaProducer
from random import randint
from threading import Thread
from time import sleep
from time import gmtime

def call_at_interval(period, callback, args):
    while True:
        sleep(period)
        callback(*args)

def setInterval(period, callback, *args):
    Thread(target=call_at_interval, args=(period, callback, args)).start()

def dataStream():
	TRAINING = 'TRAINING'
	NAMA_FITUR = ''
	NAMA_KATEGORI = random.choice(['Tunai', 'Non Tunai'])
	NAMA_GRUP = random.choice(['TarikTunai', 'TransferBRI', 'TransferBRIVA', 'TransferBersama', 'CekSaldo'])
	NAMA_SUB_GRUP = ''
	KODE_REGION = random.choice(['1', '2'])
	NAMA_REGION = ''
	if KODE_REGION == '1':	
		NAMA_REGION = 'BANDUNG'
	elif KODE_REGION == '2':
		NAMA_REGION = 'JAKARTA'
	else:
		NAMA_REGION = 'KOTA TIDAK ADA'
	DESKRIPSI_REGION = ''
	NAMA_CABANG = ''
	KODE_CABANG = random.choice(['12', '23', '34'])
	if KODE_CABANG == '12':
		NAMA_CABANG = 'ASIA AFRIKA'
	elif KODE_CABANG == '23':
		NAMA_CABANG = 'RIAU'
	elif KODE_CABANG == '34':
		NAMA_CABANG = 'AHMAD_YANI'
	NAMA_UNIT = ''
	KETERANGAN_TRX = ''
	KETERANGAN_LOKASI = ''
	NAMA_PENGIRIM = 'RIZALDI'
	BANK_PENGIRIM = 'BRI'
	REKENING_SUMBER = '121000009'
	TIPE_REKENING_SUMBER = ''
	REKENING_PENERIMA = random.choice(['121000001', '121000002', '121000003', '121000004', '121000005', '121000006', '121000007', '121000008'])
	NAMA_PENERIMA = ''
	if REKENING_PENERIMA == '121000001':
		NAMA_PENERIMA = 'azmira'
	elif REKENING_PENERIMA == '121000002':
		NAMA_PENERIMA = 'nita'
	elif REKENING_PENERIMA == '121000003':
		NAMA_PENERIMA = 'alifar'
	elif REKENING_PENERIMA == '121000004':
		NAMA_PENERIMA = 'boby'
	elif REKENING_PENERIMA == '121000005':
		NAMA_PENERIMA = 'jada'
	elif REKENING_PENERIMA == '121000006':
		NAMA_PENERIMA = 'arlita'
	elif REKENING_PENERIMA == '121000007':
		NAMA_PENERIMA = 'hendra'
	elif REKENING_PENERIMA == '121000008':
		NAMA_PENERIMA = 'mia'
	else:
		NAMA_PENERIMA = None#'nama penerima tidak ada'
	KODE_BANK_PENERIMA = ''
	KODE_MATA_UANG = ''
	ran_perc_kd_mata_uang = randint(1, 100)
	if ran_perc_kd_mata_uang<=5 and ran_perc_kd_mata_uang>0:
		KODE_MATA_UANG = '361'
	elif ran_perc_kd_mata_uang>5:
		KODE_MATA_UANG = '360'
	else:
		KODE_MATA_UANG = '0'
	SISA_UANG = None
	JUMLAH_TRX = random.choice([1000000, 2000000, 3000000, 4000000, 5000000, 10000000])
	FEE_TRANSAKSI = 0
	KODE_PROC = 12000
	KODE_RESPON = ''
	ran_perc_kd_res = random.uniform(1, 100)
	if ran_perc_kd_res>5:
		KODE_RESPON = '00'
	elif ran_perc_kd_res>2.5 and ran_perc_kd_res<=5:
		KODE_RESPON = '51'
	elif ran_perc_kd_res>=1 and ran_perc_kd_res<=2.5:
		KODE_RESPON = '61'
	else:
		KODE_RESPON = ''
	KODE_RESPONSE_DESC = ''
	TANGGAL_TRX = datetime.datetime.now().strftime('%Y-%m-%d')
	sec = int(datetime.datetime.now().strftime('%S'))
	min2sec = int(datetime.datetime.now().strftime('%M'))*60
	hour2sec = int(datetime.datetime.now().strftime('%H'))*3600

	WAKTU_TRX = sec+min2sec+hour2sec

	ID_TERMINAL = ''
	card_format = '5432'
	str_NO_KARTU = card_format+REKENING_SUMBER
	NO_KARTU = str_NO_KARTU
	now = datetime.datetime.now()
	detailed_time = gmtime()
	TAHUN = datetime.datetime.now().strftime('%Y')#.timestamp()
	BULAN = datetime.datetime.now().strftime('%M')#.timestamp()
	day_timestamp = datetime.datetime.now().strftime('%a')#.timestamp()
	DAYS = day_timestamp
	if DAYS == 'Mon':
		DAYS = 'SENIN'
	elif DAYS == 'Tue':
		DAYS = 'SELASA'
	elif DAYS == 'Wed':
		DAYS = 'RABU'
	elif DAYS == 'Thu':
		DAYS = 'KAMIS'
	elif DAYS == 'Fri':
		DAYS = 'JUMAT'
	elif DAYS == 'Sat':
		DAYS = 'SABTU'
	elif DAYS == 'Sun':
		DAYS = 'MINGGU'
	HARI = DAYS
	JAM = datetime.datetime.now().strftime('%H')
	MENIT = datetime.datetime.now().strftime('%M')
	DETIK = datetime.datetime.now().strftime('%S')
	ID_FITUR = None
	if NAMA_GRUP == 'TarikTunai':
		ID_FITUR = random.choice([103010020, 103010030, 103010040, 103010050, 103010060, 103010070, 103010080, 103010090, 103010100])
	elif NAMA_GRUP == 'CekSaldo':
		ID_FITUR = random.choice([201010170, 201010171, 201010172, 201010173, 201010180, 201010190, 201010200,201010210, 201010220, 201010230])
	elif NAMA_GRUP == 'TransferBRI':
		ID_FITUR = random.choice([205010360, 205010361, 205031660, 205010400, 205010430])
	elif NAMA_GRUP == 'TransferBersama':
		ID_FITUR = random.choice([205010370, 205010380, 205010390, 205010420, 205010440,205011420, 205031650, 205031670, 205011420])
	elif NAMA_GRUP == 'TransferBRIVA':
		ID_FITUR = random.choice([206290011])
	TIPE_TERMINAL = ''


	random_last_three = randint(0, 99)
	random_last_three_norek = '0' + str(random_last_three) if len(str(random_last_three)) == 1 else str(random_last_three)

	tipe_channel = ['ATM', 'EDC', 'INTERNET BANKING', 'SMS BANKING']
	jenis_transaksi = ['TarikTunai', 'TransferBRI', 'TransferBRIVA', 'TransferBersama']
	status_code = ['00', '51']
	amount = 50000 * randint(1, 100)
	timestamp = datetime.datetime.now().strftime('%Y-%m-%a %H:%M:%S')#.timestamp()

	result = {
			'SOURCE_FILE': TRAINING,
			'NAMA_FITUR': NAMA_FITUR,
			'NAMA_KATEGORI': NAMA_KATEGORI,
			'NAMA_GRUP': NAMA_GRUP,
			'NAMA_SUB_GRUP': NAMA_SUB_GRUP,
			'KODE_REGION': KODE_REGION,
			'NAMA_REGION': NAMA_REGION,
			'DESKRIPSI_REGION': DESKRIPSI_REGION,
			'KODE_CABANG': KODE_CABANG,
			'NAMA_CABANG': NAMA_CABANG,
			'NAMA_UNIT': NAMA_UNIT,
			'KETERANGAN_TRX': KETERANGAN_TRX,
			'KETERANGAN_LOKASI': KETERANGAN_LOKASI,
			'NAMA_PENGIRIM': NAMA_PENGIRIM,
			'BANK_PENGIRIM': BANK_PENGIRIM,
			'REKENING_SUMBER': REKENING_SUMBER,
			'TIPE_REKENING_SUMBER': TIPE_REKENING_SUMBER,
			'REKENING_PENERIMA': REKENING_PENERIMA,
			'NAMA_PENERIMA': NAMA_PENERIMA,
			'KODE_BANK_PENERIMA': KODE_BANK_PENERIMA,
			'KODE_MATA_UANG': KODE_MATA_UANG,
			'SISA_UANG': SISA_UANG,
			'JUMLAH_TRX': JUMLAH_TRX,
			'FEE_TRANSAKSI': FEE_TRANSAKSI,
			'KODE_PROC': KODE_PROC,
			'KODE_RESPON': KODE_RESPON,
			'KODE_RESPONSE_DESC': KODE_RESPONSE_DESC,
			'TANGGAL_TRX': TANGGAL_TRX,
			'WAKTU_TRX': WAKTU_TRX,
			'ID_TERMINAL': ID_TERMINAL,
			'NO_KARTU': NO_KARTU,
			'TAHUN': TAHUN,
			'BULAN': BULAN,
			'HARI': HARI,
			'JAM': JAM,
			'MENIT': MENIT,
			'DETIK': DETIK,
			'ID_FITUR': ID_FITUR,
			'TIPE_TERMINAL': TIPE_TERMINAL,
		}

	print(result)
		
	#	ENCODE DATA AS BINARY DATA
	data = result
		

	bytes_writer = io.BytesIO()
	encoder = avro.io.BinaryEncoder(bytes_writer)
	writer.write(data, encoder)
		
	raw_bytes = bytes_writer.getvalue()

	print(raw_bytes)
	
#	SEND BINARY DATA TO PRODUCED KAFKA TOPIC "test_producer"
	producer = KafkaProducer(bootstrap_servers='10.10.10.204:9092')
	producer.send('rizaldi',raw_bytes)

#	print(raw_bytes)

#__________________________MAIN__________________________

if	__name__ == '__main__':

	##PARAMETERS
	producer_schema = avro.schema.Parse(open("avro-schema-fix.avsc").read())
	writer = avro.io.DatumWriter(producer_schema)
	
	try:
		setInterval(1, dataStream)
	except KeyboardInterrupt:
		print("KeyboardInterrupt")
		sys.exit()
		close()