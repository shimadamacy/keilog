#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WiSUNドングル RL7023 Stick-D/DSS または IPS を用いてスマートメーターから電力情報を取得する。
"""

import sys
import serial
import time
import datetime
import os
import json
import threading
import queue
from abc import ABCMeta, abstractmethod

from keilib.worker import Worker

from logging import getLogger, StreamHandler, DEBUG
logger = getLogger(__name__)

reginfo = {
    'S01': 'MAC address',     #'MACアドレス(64bit)',
    'S02': 'channel number',  #'チャンネル番号(33-60)',
    'S03': 'PAN ID',          #'PAN ID(16bit)',
    'S07': 'frame counter',   # 'フレームカウンタ(32bit)',
    'S0A': 'Pairing ID',           #(8文字)',
    'S0B': 'Pairing ID(HAN)',      #(16文字)',
    'S15': 'beacon response flag', #'ビーコン応答フラグ(0:無視、1:応答)',
    'S16': 'PANA session life time',  #'PANAセッションライフタイム(秒:0x60〜0xFFFFFFFF)',
    'S17': 'auto rejoin flag',        #'自動再認証フラグ(0:無効、1:再認証自動実行)',
    'S1C': 'PAA key update cycle time', #'PAA鍵更新周期(HAN)(秒:0x60〜0x2592000)',
    'S1F': 'relay device MAC address', # 'リレーデバイスアドレス',
    #'S64': 'アンテナ切り替え',
    'SA1': 'ICMP response flag',       # 'ICMPメッセージ処理制御(平文のメッセージ 0:破棄、1:受入処理)',
    'SA2': 'ERXUDP event style', # 'ERXUDPイベント形式フラグ(1:RSSI含める, 0:含めない)',
    'SA9': 'transmition and receive enabled', #'送受信有効フラグ(0:無効、1:有効)',
    'SF0': 'active side', #'アクティブMAC面の指定(0:B面、1:H面)',
    'SFB': 'transmition restriction flag', # '送信時間制限中フラグ(1:制限中)',
    'SFD': 'transmition total time', #'無線送信の積算時間(ms)',
    'SFE': 'echo back flag',#'エコーバックフラグ(0:なし、1:あり)',
    'SFF': 'auto load',#'オートロード(0:無効、1:有効)'
}

def is_hex( data, length=0 ):
    """妥当な16進数かどうかをチェック
    引数:
        data (str):チェックする文字列
        length (int) = 0: 指定すると長さチェックも行う
    戻り値:
        True/False
    """
    if length:
        if len(data) != length:
            return False
    for ch in data:
        if not ch in '0123456789ABCDEF':
            return False
    return True

def is_ipv6_address( addr ):
    """妥当なipv6アドレスであるかチェック
    引数:
        addr (str): チェックするアドレス文字列
    戻り値:
        True/False
    """
    lst = addr.split(':')
    if len(lst) != 8:
        return False
    for word in lst:
        if len(word) != 4:
            return False
        if not is_hex(word):
            return False
    return True

def hex_to_signed_int(value, digit=0):
    """16進文字列データを2の補数による unsigned int として整数に変換する
    引数:
        value (str): 変換する16進表記文字列
        digit (int)=0: 16進数の桁数。省略するとvalueの長さを桁数とみなす
    戻り値:
        変換結果の整数
    """
    if digit == 0:
        digit = len(value)
    digit2 = digit * 4
    fmtstr = '{:0' + str(digit2) + 'b}'
    bits = fmtstr.format(int(value,16))
    return -int(bits[0]) << digit2 | int(bits, 2)

class WiSunDevice ( metaclass=ABCMeta ):
    """WiSUN デバイスドライバ抽象クラス

    以下のメソッドをインプリメントすると BrouteReader から利用できる
    """
    @abstractmethod
    def open( self ):
        """デバイスのオープン,ハードウェアの認識
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def reset( self ):
        """デバイスのリセット,レジスタ等を初期化
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def setup( self, id, password ):
        """アクティブスキャンの準備（必要な情報をセット）
        引数: id/password BルートID/パスワード
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def scan( self ):
        """アクティブスキャンの実行、結果の保持
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def join( self ):
        """PANA認証の実行
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def rejoin( self ):
        """PANA再認証の実行
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def sendto( self, dataframe ):
        """Echonet Lite データフレームの送信
        引数:
            dataframe: Echonet 電文（DataFrame オブジェクト）
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def receive( self ):
        """Echonet Lite データフレームの受信
        戻り値: 受信した電文からつくられた DataFrame オブジェクト
        """
        pass

    @abstractmethod
    def term( self ):
        """PANAセッションの終了
        戻り値: True（成功）/False（失敗）
        """
        pass

    @abstractmethod
    def close( self ):
        """デバイスのクローズ,デバイスの開放
        戻り値: True（成功）/False（失敗）
        """
        pass

class WiSunRL7023 ( WiSunDevice ):
    """RL7023 Stick-D 用デバイスドライバ

    テセラテクノロジーの HEMS 用 Wi-SUN モジュール Route-B/HAN デュアル対応
    RL7023 Stick-D/DSS または シングルタイプの D/IPS を制御するクラス。
    """
    IPS=0
    DSS=1
    def __init__( self, port, baud , type=DSS):
        """コンストラクタ
        引数:
            port (str): RL7023 のシリアルポートを示すファイルパス
            baud (int): 通信ボーレート
        """
        self.port = port
        self.baud = baud
        self.type = type
        self.register = {}
        self.scanresult = {}

        self._TIMEOUT_MAX = 20
        self._TIMEOUT_SCAN = 300

    def _wait_ok( self ):
        """デバイスから返り値 OK を待つ
        戻り値:
            True: OKを得られた
            False: OK以外の文字を得たかタイムアウトした
        """
        toc = 0
        while True:
            res = self.ser.readline()
            if res[:2] == b'OK':
                logger.debug('OK')
                return True
            elif res ==  b'':
                toc += 1
                if toc > self._TIMEOUT_MAX:
                    logger.debug('time out')
                    return False
            else:
                # それ以外は読み飛ばし。エコーバック等が来る
                pass

    def _set_panid( self, pan_id ):
        """デバイスのレジスタ（S3）に Pan ID をセットする

        戻り値:
            True: 成功
            False: 失敗
        """
        cmd = 'SKSREG S3 ' + pan_id + '\r\n'
        logger.info(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        if self._wait_ok():
            return True
        else:
            return False

    def _set_channel( self, channel ):
        """デバイスのレジスタ（S2）に Channel をセットする

        戻り値:
            True: 成功
            False: 失敗
        """
        cmd = 'SKSREG S2 ' + channel + '\r\n'
        logger.info(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        if self._wait_ok():
            return True
        else:
            return False

    def _readline( self ):
        """SKデバイスから１行読み込む

        戻り値:
            読み込みデータ
        """
        return self.ser.readline()

    def _parse_event( self, line ):
        """読み取った一行のイベントデータを、スペースを区切り文字として分解し、チェックして辞書に登録

        戻り値:
            変換した結果辞書（空の文字列に対しては空の辞書を返す）

        ToDo:
            ERXUDP, EVENT 以外のイベントへの対応
        """
        # 有効でないASCII文字コードを含む場合は終了
        for b in line:
            if b >= 0x80:
                return {'NAME': 'INVALID_EVENT'}

        # 行をリストに変換
        list = line.decode('ascii').strip().split()
        if len(list) == 0:
            return {}


        # 1. ERXUDP イベントの場合
        # ERXUDP <SENDER> <DEST> <RPORT> <LPORT> <SENDERLLA> <SECURED> <SIDE> <DATALEN> <DATA><CRLF>
        # 0      1         2      3       4       5           6         7      8         9
        """
        【 SKコマンドにおける ERXUDP イベント の構成 】
            0 ERXUDP
            1 <SENDER>    XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX
            2 <DIST>      XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX:XXXX
            3 <RPORT>     0E1A
            4 <LPORT>     0E1A
            5 <SENDERLLA> XXXXXXXXXXXXXXXX
            6 <SECURED>   1
            7 <SIDE>      0
            8 <DATALEN>   0012
            9 <DATA>      1081000102880105FF017201E704000004A5
        <DATA> 部が Echonet データフレーム

        参考文献:
            SKIP_Command_dse_v1_02a.pdf（商品を購入して、製品登録すれば入手できる）
        """
        if list[0] == 'ERXUDP':
            idx = 0
            if self.type == self.DSS:
                length = 10
            else:
                length = 9

            # データ数のチェック
            if len(list) != length:
                return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            # IPV6 アドレスとして正しいかどうかチェック
            for i in [1,2]:
                if not is_ipv6_address(list[i]):
                    logger.debug('invalid IPV6')
                    return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            # ポート番号、4byte であるか
            for i in [3,4]:
                if not is_hex(list[i], length=4):
                    logger.debug('invalid PORT')
                    return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            # SENDERLLA ローカルアドレスが 16byte であるか
            if not is_hex(list[5], length=16):
                logger.debug('invalid SENDERLLA')
                return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            # SECURED、が 1byte であるか
            if not is_hex(list[6], length=1):
                logger.debug('invalid SECURED')
                return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            idx = 7
            if self.type == self.DSS:
                # SIDE が 1byte であるか
                if not is_hex(list[7], length=1):
                    logger.debug('invalid SIDE')
                    return {'NAME': 'INVALID_ERXUDP', 'LIST': list}
                idx = 8
            else:
                idx = 7

            # DATALEN が 4byte であるか
            if not is_hex(list[idx], length=4):
                logger.debug('invalid DATALEN')
                return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            # DATAが HEX 値として読み取れる文字列か
            if not is_hex(list[idx+1]):
                logger.debug('invalid DATA')
                return {'NAME': 'INVALID_ERXUDP', 'LIST': list}

            dict = {'NAME':list[0], 'SENDER': list[1], 'DEST': list[2], 'RPORT': list[3],
                    'LPORT': list[4], 'SENDERLLA': list[5], 'SECURED': list[6],
                    'DATALEN': list[idx], 'DATA': list[idx+1]}

            return dict

        # 2. EVENT の場合
        elif list[0] == 'EVENT':
            if len(list) < 3:
                return {}

            #dict = {'NAME': list[0], 'NUM': list[1], 'SENDER': list[2], 'SIDE': list[3]}
            dict = {'NAME': list[0], 'NUM': list[1], 'SENDER': list[2]}
            #if len(list) > 3:
            #    dict['PARAM'] = list[4]
            #    return dict
            return dict

        # 3. その他のイベントの場合（EPONG, EADDR, ENEIGHBOR, EPANDESC, EEDSCAN, ESEC, ENBR）
        else:
            return {'NAME': 'OTHER_EVENT', 'LIST': list}

        return dict

    def _get_event( self ):
        """デバイスからイベントを読み取る

        戻り値:
            読み込んだイベント辞書（タイムアウトの場合は空の辞書）
        """
        # SKデバイスから一行読み取る。タイムアウト設定あり。self._readline()
        line = self._readline().strip()
        event = self._parse_event(line)

        if event:
            logger.debug('---- event info -----')
            logger.debug(line.decode('ascii'))
            logger.debug(event)

        return event

    def _set_password( self, broute_pwd ):
        """デバイスにパスワードを登録する
        戻り値:
            True: 成功
            False: 失敗
        """
        cmd = 'SKSETPWD C ' + broute_pwd + '\r\n'
        logger.debug(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        if self._wait_ok():
            return True
        else:
            return False

    def _set_id( self, broute_id ):
        """デバイスにBルート認証IDを登録する
        戻り値:
            True: 成功
            False: 失敗
        """
        cmd = 'SKSETRBID ' + broute_id + '\r\n'
        logger.debug(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        if self._wait_ok():
            return True
        else:
            return False

    def _get_registers( self ):
        """デバイスのレジスタの値を読み出して記憶する。"""
        for key, description in sorted(reginfo.items()):
            cmd = 'SKSREG ' + key + '\r\n'
            self.ser.write(cmd.encode('ascii'))

            toc = 0
            while True:
                res = self.ser.readline()
                if res[:6] == b'ESREG ':
                    val = res.decode('ascii').strip().split()[1]
                    self.register[key] = val
                    logger.info(key + ' ' + description + ' : ' + val )

                else:
                    pass

                if res[:2] == b'OK':
                    break

                toc += 1
                if toc > 5:
                    break

    def _scancache( self ):
        """スキャン結果のキャッシュが一時間以内であれば、それを使う"""
        if os.path.exists('scancache.json'):
            mtime = os.stat('scancache.json').st_mtime
            now = datetime.datetime.now().timestamp()
            json_data = {}
            if now - mtime < 3600:
                try:
                    with open("scancache.json", 'r') as f:
                        json_data = json.load(f)
                    if {'Pan ID','Channel','Addr'} <= json_data.keys():
                        return json_data
                    else:
                        os.remove('scancache.json')
                        return {}
                except:
                    return {}
            else:
                return{}
        else:
            return {}

    def _scanexec( self ):
        """アクティブスキャンを実施する

        ToDo:
            sideを省略すれば片面用になるか？
        """

        # SKSCANコマンドの設定
        mode     = 2 # アクティブスキャン（Information Element あり）
        mask     = 'FFFFFFFF' # スキャンするチャンネルを指定するマスク(32bit) 最下位ビットが ch33
        duration = 7 # スキャン時間 1増えると2倍の時間がかかる
        side     = 0 # 0: B route

        scanresult = {}
        # Step1 デバイスにスキャンコマンドを送る
        if self.type == self.DSS:
            cmd = 'SKSCAN ' + str(mode) + ' ' + mask + ' ' \
                            + str(duration) + ' ' + str(side) + '\r\n'
        else:
            cmd = 'SKSCAN ' + str(mode) + ' ' + mask + ' ' \
                            + str(duration) + '\r\n'

        logger.info(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        # デバイスからOKが返されるのを待つ
        if not self._wait_ok():
            return False

        # Step2 スキャン結果がイベントとして返されるのを待つ
        # 様々なイベントが発生するのでそれぞれ処理する
        toc = 0
        while True:
            # 一行読み取る（タイムアウト1秒）
            res = self.ser.readline()

            if res == b'':
                # データ無し。タイムアウト時など。
                toc += 1
                if toc > self._TIMEOUT_SCAN:
                    return False

            elif res[:8] == b'EVENT 22':
                # EVENT 22:アクティブスキャンが完了 -> ループ終了
                logger.info('EVENT: 22')
                break

            elif res[:8] == b'EVENT 20':
                # EVENT 20:Beaconを受信した
                # 直後に EPANDESC イベントが発生し、スキャン結果が表示される
                logger.info('EVENT: 20')

            elif res[:8] == b'EPANDESC':
                # EPANDESC:スキャンで発見したPANを通知するイベント（一旦改行される）
                logger.info('EPANDESC')

            elif res[:2] == b'  ':
                # EPANDESC イベントに続いてスキャン結果が流れてくるので読み込んでゆく
                # 行頭のスペース2個に続いて [param]：[value]+<crlf>が繰り返して送られる
                # [param] = "Channel","Channel Page","Pan ID","Addr","LQI","PairID"
                lst = res.decode('ascii').strip().split(':')
                logger.info('  ' + res.decode('ascii').strip())
                scanresult[lst[0]] = lst[1]

            else:
                logger.info('Unkown EVENT : ' + res.decode('ascii'))

        logger.debug('active scan result')
        logger.debug(scanresult)
        if {'Pan ID','Channel','Addr'} <= scanresult.keys():
            # スキャン結果に必要な情報が含まれている場合
            with open("scancache.json", 'w') as f:
                # スキャン結果のキャッシュへの書込み
                json.dump(scanresult, f, indent=4)
            return scanresult

        else:
            return {}

    def open( self ):
        """ デバイスのシリアルポートをオープンする。
        """
        self.ser = serial.Serial(
            port     = self.port,
            baudrate = self.baud,
            bytesize = serial.EIGHTBITS,
            parity   = serial.PARITY_NONE,
            stopbits = serial.STOPBITS_ONE,
            timeout  = 1,
            xonxoff  = False,
            rtscts   = False,
            dsrdtr   = False
        )
        # self.ser.timeout = 1
        logger.info('SKDevice open port={}, baud={}'.format(self.port, self.baud))
        return True

    def reset( self ):
        """デバイスにリセットコマンドを送る
        戻り値:
            True: 成功
            False: 失敗
        """
        cmd = 'SKRESET\r\n'
        logger.debug(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        if self._wait_ok():
            return True
        else:
            return False

    def setup( self, id, password ):
        """デバイスにBルートIDおよびパスワードを登録し、スキャンの前準備
        戻り値:
            True: 成功
            False: 失敗
        """
        r1 = self._set_password(password)
        r2 = self._set_id(id)
        if r1 and r2:
            return True
        else:
            return False

    def scan( self ):
        """アクティブスキャンを実行する。結果を self.scanresult 辞書に格納
        戻り値:
            True: 成功
            False: 失敗
        """
        self.scanresult   = {}

        # Step1 スキャン結果のキャッシュがあればそれを使う,なければスキャン実行
        self.scanresult = self._scancache()
        if not self.scanresult:
            self.scanresult = self._scanexec()
            if not self.scanresult:
                # スキャン失敗
                return False

        # Step2 スキャン結果を使ったデバイスの設定（通信先の情報を設定する）
        if {'Pan ID', 'Channel', 'Addr'} <= self.scanresult.keys():
            # スキャン結果の核心部がちゃんと取得できてる

            # SKデバイスのレジスタに、スキャンで得られたPSN_IDとチャネル番号を設定する。
            self._set_panid(self.scanresult['Pan ID'])
            self._set_channel(self.scanresult['Channel'])

            # SKLL64コマンド: 64ビットMACアドレスをIP_V6アドレスに変換する
            cmd = 'SKLL64 ' + self.scanresult['Addr'] + '\r\n'
            logger.info(cmd.strip())
            self.ser.write(cmd.encode('ascii'))
            self.ser.readline() #エコーバック読み飛ばし
            self.ipv6_addr = self.ser.readline().strip().decode('ascii')
            logger.info('IP_ADDR = ' + self.ipv6_addr)

            # ここまできたらスキャン成功とする
            return True

        else:
            # 実はスキャン成功してなかった
            self.scanresult = {}
            return False

    def join( self , rejoin=False):
        '''PANA認証クライアントとしてPANA認証シーケンスを開始

        引数:
            rejoin (bool): リジョインの場合はTrue（デフォルトはTrue）

        戻り値:
            True: 成功
            False: 失敗
        '''
        # Step1 デバイスに SKJOIN コマンドを送信する。 指定したIP6アドレスとPANA認証シーケンスを開始する。
        if rejoin:
            cmd = 'SKREJOIN\r\n'
        else:
            cmd = 'SKJOIN ' + self.ipv6_addr + '\r\n'
        logger.info(cmd.strip())
        self.ser.write(cmd.encode('ascii'))

        # Step2 イベントを監視し、接続先からの返答を待つ EVENT 24が得られるとJoin成功
        toc = 0
        while True:
            event = self._get_event()

            if event:
                if event['NAME'] == 'EVENT':
                    if event['NUM'] == '25':
                        # EVENT 25: PANAによる接続が完了した。
                        logger.info('EVENT: 25 - JOIN SUCCEED')
                        return True

                    elif event['NUM'] == '24':
                        # EVENT 24: PANAによる接続過程でエラーが発生した
                        logger.info('EVENT: 24 - JOIN FAILED')
                        return False

                    else:
                        logger.info('EVENT: ' + event['NUM'])
                        logger.debug(event)

                elif event['NAME'] == 'ERXUDP':
                    logger.info('ERXUDP')
                    logger.debug(event)

                else:
                    logger.info(event)
                    pass

            else:
                # データ無し、一定回数でタイムアウト
                toc += 1
                logger.debug('no evnet')
                if toc > self._TIMEOUT_MAX:
                    logger.info('timeout')
                    return False

    def rejoin( self ):
        """PANA認証状態で再認証を行い、暗号化キーの更新を行う

        RL7023 Stick-D/DSS はデフォルトで自動再認証を行うため通常は必要ない。

        戻り値:
            True: 成功
            False: 失敗
        """
        return self.join(rejoin=True)

    def sendto( self, dataframe ):
        """スマートメーターにコマンドを送信する

        引数:
            dataframe(byte): スマートメーターに送信する Echonet電文

        戻り値:
            True: 成功
            False: 失敗

        ToDo:

        """
        byteframe = bytes.fromhex(dataframe.encode())
        # コマンド送信
        #                   udp_handle
        #                   | addr port
        #                   |  |   |   security 1:encrypt
        #                   |  |   |   | side 0:B-route
        #                   |  |   |   | | length
        #                   |  |   |   | | |
        if self.type == self.DSS:
            cmd = "SKSENDTO 1 {0} 0E1A 1 0 {1:04X} ".format(self.ipv6_addr, len(byteframe)).encode('ascii')
        else:
            cmd = "SKSENDTO 1 {0} 0E1A 1 {1:04X} ".format(self.ipv6_addr, len(byteframe)).encode('ascii')
        cmd += byteframe # + b'\r\n'
        self.ser.write(cmd)
        evt21 = False
        toc = 0
        while True:
            res = self.ser.readline()
            if res == b'':
                toc += 1
                if toc > self._TIMEOUT_MAX:
                    logger.debug('timeout')
                    return False

            elif res[:8] == b'EVENT 21':
                logger.debug(res.decode('ascii').strip())
                evt21 = True

            elif res[:2] == b'OK':
                logger.debug(res.decode('ascii').strip())
                return evt21

            else:
                #logger.debug(res.decode('ascii').strip())
                logger.debug('unknown response')

    def receive( self ):
        """スマートメーターからの電文を受信する

        戻り値:
            スマートメーターからの電文に対応する DataFrame オブジェクト
        """

        event = self._get_event()

        # 受信イベント処理
        if event:
            if event['NAME'] == 'ERXUDP':

                dataframe = DataFrame.decode(event['DATALEN'], event['DATA'])

                if dataframe:
                    logger.debug([event['DATA'], dataframe.endict()])
                    return dataframe

                else:
                    logger.error('invalid ERXUDP data frame')
                    logger.error(event)
                    pass

            # ERXUDP以外のイベントの処理
            else:
                logger.warning('other EVENT')
                logger.warning(event)
                pass

        return None

    def term( self ):
        """デバイスに SKTERM コマンドを送り、PANAセッションの終了を要請

        戻り値:
            True: 成功
            False: 失敗
        """
        cmd = 'SKTERM\r\n'
        logger.info(cmd.strip())
        self.ser.write(cmd.encode('ascii'))
        if self._wait_ok():
            #return True
            pass
        else:
            return False

        toc = 0
        while True:
            event = self._get_event()

            if event:
                if event['NAME'] == 'EVENT':
                    if event['NUM'] == '27':
                        # EVENT 27: セッション終了が成功
                        logger.info('EVENT: 27 - TERM SUCCEED')
                        return True

                    elif event['NUM'] == '28':
                        # EVENT 28: タイムアウトしてセッション終了となった
                        logger.info('EVENT: 28 - TERM TIMEOUT, Session terminate')
                        return True

                    else:
                        logger.info('EVENT: ' + event['NUM'])
                        logger.debug(event)

                elif event['NAME'] == 'ERXUDP':
                    logger.info('ERXUDP')
                    logger.debug(event)
                else:
                    logger.info(event)
                    pass
            else:
                # データ無し、一定回数でタイムアウト
                toc += 1
                logger.debug('no evnet')
                if toc > self._TIMEOUT_MAX:
                    logger.info('timeout')
                    return False

    def close( self ):
        """デバイスのシリアルポートをクローズする"""
        self.ser.close()
        logger.info('skdevice closed. ')

class DataFrame ( ):
    '''EchonetBroute用のデータフレームを定義する

    【 EchonetBroute-Lite データフレーム の構成 】
    0    1    2      3     4   5   6   7   8
    ------------------------------------------------
    EHD  TID  SEOJ   DEOJ  ESV OPC EPC PDC EDT
    1081 0001 028801 05FF01 72 01  E7  04  000004A5
    ------------------------------------------------
    0 EHD: Echonet Lite header 0x1081 で EchonetBroute Lite。スマートメータはこれで固定
    1 TID: Transaction ID 要求-応答を対応付ける番号（要求時に自由につける）
    2 SEOJ: Sender Echonet ObJect (id) 送り手側機器オブジェクトID
    3 DEOJ: Destination Echonet ObJect (id) 宛先側機器オブジェクトID
        機器オブジェクトIDの例
            05FF01 = Raspi側
                05: 管理グループコード
                FF: コントローラークラス
                01: インスタンス番号
            028801 = スマートメータ側
                02: 住宅・設備関連機器クラスグループ
                88: 低圧スマートメータークラス
                01: インスタンス番号
            0EF001
                ? ノードプロファイルオブジェクト？
    4 ESV: Echonet Lite Service
            0x62＝プロパティ値 読み出し要求
            0x72＝プロパティ値 読み出し応答
            0x73＝プロパティ値 通知（要求なしで定期的に通知）
            他にもいろいろある
    5 OPC: 処理対象プロパティカウンタ（以下のEPC/PDC/EDTがこの数だけ繰り返される）
    -----繰り返し-----
    6 EPC: Echonet プロパティ 機器ごとにプロパティコードが定められている、スマートメータークラスでは0xE7が1W単位の瞬時電力
    7 PDC: プロパティデータカウンタ プロパティのデータサイズ、例えば４バイト プロパティ要求のときは00を送る
    8 EDT: プロパティ値データ 例えば瞬時電力の値 0x000004A5 = 1189W プロパティ要求のときは省略
    -----繰り返し-----

    参考文献
        ECHONET-Lite_Ver.1.13_02.pdf ECHONET Lite 通信ミドルウェア仕様
        Appendix_Release_K.pdf ECHONET 機器オブジェクト詳細規定
    '''

    # トランザクションID
    TID = 0

    def __init__( self, dataframe={} ):
        """コンストラクタ"""
        #self.dataframe = dataframe
        if dataframe:
            self.ehd = dataframe['EHD']
            self.tid = dataframe['TID']
            self.seoj = dataframe['SEOJ']
            self.deoj = dataframe['DEOJ']
            self.esv = dataframe['ESV']
            self.properties = {}
            for prop in dataframe['PROPERTIES']:
                #prop['EPC']: prop[]
                self.properties[prop['EPC']] = prop['EDT']

    @classmethod
    def decode ( cls, length, encoded_data ):
        """受信した電文（HEX文字データ列）からインスタンスを作成する

        引数:
            length (str):        電文の長さ（16進表記 2byte 文字列）
            encoded_data (str):  電文の本体（16進表記の文字列）

        戻り値:
            DataFrame インスタンス

        """

        dataframe = {}
        if len(encoded_data) != int(length, 16) * 2:
            logger.error('DataFrame decode error - invalid length')
            return None

        if not is_hex(encoded_data):
            logger.error('DataFrame decode error - invalid data')
            return None

        df = cls()
        try:
            df.ehd = encoded_data[:4]
            df.tid = encoded_data[4:8]
            df.seoj = encoded_data[8:14]
            df.deoj = encoded_data[14:20]
            df.esv = encoded_data[20:22]
            df.opc = encoded_data[22:24]
            df.properties = {}
            base = 24
            int_opc = int(df.opc, 16)
            for pc in range(int_opc):
                epc = encoded_data[base     : base + 2]
                pdc = encoded_data[base + 2 : base + 4]
                int_pdc = int(pdc, 16)
                edt = encoded_data[base + 4 : base + 4 + int_pdc * 2]
                df.properties[epc] = edt
                base += 4 + int_pdc * 2

        except:
            logger.error('DataFrame decode error - conflicting data')
            return None

        return df

    @classmethod
    def cmd_get_property( cls , epc_list ):
        '''Echonet Bルートの「プロパティ値要求電文」を作成する。

        引数:
            epc_code_list( list of str ): 要求するプロパティコードのリスト

        戻り値:
            構成されたデータフレームオブジェクト

        ToDo:
            SEOJやDEOJは決め打ちでいいのか？
        '''
        df = cls()
        df.ehd = '1081'
        df.tid = '{:04X}'.format(cls.TID)
        # cls.TID  = (cls.TID + 1) % 0xffff       # インクリメントしておく
        df.seoj = '05FF01'
        df.deoj = '028801'
        df.esv = '62'
        df.opc = '{:02X}'.format(len(epc_list))
        df.properties = {}
        for epc in epc_list:
            df.properties[epc] = ''
        logger.debug('Echonet-lite sendto frame : ' + df.encode())
        return df

    def encode( self ):
        """DataFrameオブジェクトから送信電文(HEX)を作成する
        """
        data = self.ehd + self.tid + self.seoj + self.deoj + self.esv + self.opc
        for epc, edt in self.properties.items():
            pdc = '{:02X}'.format(len(edt) // 2)
            data += epc + pdc + edt

        return data

    def endict( self ):
        """DataFrameオブジェクトを見やすい辞書形式に変換する"""
        dict = {}
        dict['EHT'] = self.ehd
        dict['TID'] = self.tid
        dict['SEOJ'] = self.seoj
        dict['DEOJ'] = self.deoj
        dict['ESV'] = self.esv
        dict['OPC'] = self.opc
        dict['PROPERTIES'] = {}
        for prop, value in self.properties.items():
            dict['PROPERTIES']['EPC'] = prop
            dict['PROPERTIES']['EDT'] = value
            dict['PROPERTIES']['PDC'] = '{:02X}'.format(len(value) // 2)

        return dict

class BrouteReader ( Worker ):
    """スマートメーターと通信を行い、継続的に電力情報の読み取りを行う。

    デバイスの状態を管理し、状態に応じた動作を定義している

    状態遷移図:
        INIT (初期状態)
        |
        |  _open() WiSUNドングルのシリアルポートの設定など
        |
        OPEN (ドングル利用可能な状態)
        |
        |  _setup() ドングルのリセット、ドングルのレジスタににBルートIDとパスワードをセット
        |
        SETUP (スキャンに必要な接続情報が設定された状態)
        |
        |  _scan() スマートメーターの検索、アドレスの取得
        |
        SCAN (通信相手が見つかって、あとは認証して接続するだけの状態)
        |
        |  _join() 相手に接続を要請（PANA認証）
        |
        JOIN (接続状態にある)
        |
        |  _sendto(cmd) 「プロパティ値読み出し要求」コマンドを送る（瞬時電力や積算電力）
        |  _receive() 「プロパティ値読み出し応答」スマートメーターからの返信を取得
        |  _accept()   返信データを処理
        |
        以下 JOIN の繰り返し

        回復不能なエラーが発生したときは、状態を巻き戻し最初からやり直す。
    """

#    def __init__( self , port, baudrate, broute_id, broute_pwd, requests=[], record_que=None ):
    def __init__( self , wisundev, broute_id, broute_pwd, requests=[], record_que=None ):
        """コンストラクタ

        引数:
            port (str): WiSUN デバイスのシリアルポートのデバイスファイルパス
            baudrate (int): ボーレート
            broute_id (str): Bルート接続用ID（電力会社に申請して取得）
            broute_pwd (str): Bルート接続用パスワード（電力会社に申請して取得）
            requests (list of dic): スマートメータに問い合わせるプロパティリスト、間隔
            record_que (Queue): 記録用 queue
        """
        super().__init__()

        self.record_que = record_que
        self.broute_id = broute_id
        self.broute_pwd = broute_pwd
        if not requests:
            # 指定されなかったときのデフォルト値
            requests=[
                { 'epc':['D3','D7','E1'], 'cycle': 3600 }, # 係数(D3),有効桁数(D7),単位(E1)
                { 'epc':['E7'], 'cycle': 10 }, # 瞬時電力(E7)
                { 'epc':['E0'], 'cycle': 120 }, # 積算電力量(E0)
            ]
        for req in requests:
            req['lasttime'] = 0
        self.requests = requests
        #self.wisundev = WiSunRL7023DSS( port, baudrate )
        self.wisundev = wisundev

        # 状態
        self._STATE_INIT  = 0
        self._STATE_OPEN  = 1
        self._STATE_SETUP = 2
        self._STATE_SCAN  = 3
        self._STATE_JOIN  = 4
        self.state = self._STATE_INIT

        # リトライカウンタ
        self.scan_retry = 0
        self.join_retry = 0

        # 係数、単位、有効桁数 のデフォルト値 時々スマートメーターに問い合わせるべきもの
        self.coefficient = 1
        self.unit = 0.1
        self.effective_digits = 0x06

        # 定期的な実行のための変数（前回実行した時間を記憶しておく）
        self.lasttime_rejoin = 0
        self.lasttime_erxudp = 0
        self.lasttime_receive = 0

    def _open( self ):
        """デバイスのシリアルポートをopenする"""
        logger.info('state = INITIAL')
        if self.wisundev.open():
            self.state = self._STATE_OPEN
            logger.info('state => OPEN')
        else:
            logger.error('ERROR Cannot open device')
            # エラーの場合は1秒停止。無限ループがCPUを専有しないように。（以下同様）
            time.sleep(5)

    def _setup( self ):
        """デバイスのリセット、BルートID、パスワードの設定
        """
        # リセット
        if self.wisundev.reset():
            pass
        else:
            logger.error('ERROR Cannot reset device')
            time.sleep(5)
            # break

        # BルートIDとパスワードをデバイスに設定（レジスタに登録される）
        if self.wisundev.setup( self.broute_id, self.broute_pwd ):
            self.state = self._STATE_SETUP
            logger.info('state => SETUP')
            self.wisundev._get_registers()
        else:
            logger.error('ERROR Cannot setup device')
            time.sleep(5)

    def _scan( self ):
        """アクティブスキャンの実行（リトライあり）"""
        if self.wisundev.scan():
            self.state = self._STATE_SCAN
            logger.info('state => SCAN')
            self.scan_retry = 0
        else:
            # 失敗してもただちに状態を遷移させず、ループで何度かトライ
            logger.error('ERROR Fail to scan ... retry times = ' + str(self.scan_retry))
            self.scan_retry += 1
            if self.scan_retry > 5:
                self.scan_retry = 0
                self.wisundev.close()
                self.state = self._STATE_INIT
            time.sleep(10)

    def _join( self ):
        """PANA認証接続要求を行う（リトライあり）"""
        if self.wisundev.join():
            self.state = self._STATE_JOIN
            logger.info('state => JOIN')
            self.join_retry = 0

            ts = datetime.datetime.now().timestamp()
            self.lasttime_erxudp = ts
            self.lasttime_rejoin = ts
            self.lasttime_receive = ts

        else:
            # 失敗してもただちに状態を遷移させず、ループで何度かトライ
            logger.error('ERROR Fail to join ... retry times = ' + str(self.join_retry))
            self.join_retry += 1
            if self.join_retry > 5:
                self.join_retry = 0
                self.wisundev.close()
                try:
                    os.remove('scancache.json')
                except:
                    pass
                self.state = self._STATE_INIT
            time.sleep(10)

    def _rejoin( self ):
        """PANA再認証を行う
        セッションの有効期限が近づくとデバイスが自動的に再認証を行う設定になっている場合は不要
        """
        if self.wisundev.rejoin():
            self.state = self._STATE_JOIN
            logger.info('state => JOIN')

            ts = datetime.datetime.now().timestamp()
            self.lasttime_erxudp = ts
            self.lasttime_rejoin = ts
            self.lasttime_receive = ts
        else:
            self.state = self._STATE_INIT
            time.sleep(5)

    def _sendto( self, cmd ):
        """Bルートのプロパティ値要求電文をスマートメーターに送る"""
        return self.wisundev.sendto( cmd )

    def _receive ( self ):
        return self.wisundev.receive()

    def _accept( self, dataframe ):
        """受信した電文を受け付ける処理"""

        def getvalue(rawdata):
            value = int(rawdata, 16)
            return value * self.coefficient * self.unit

        def datestr(rawdata):
            year = int(rawdata[:4],16)
            month = int(rawdata[4:6],16)
            day =int (rawdata[6:8],16)
            hour = int(rawdata[8:10],16)
            minute = int(rawdata[10:12],16)
            second = int(rawdata[12:14],16)
            return "{:0=4}/{:0=2}/{:0=2} {:0=2}:{:0=2}:{:0=2}".format(year,month,day,hour,minute,second)

        seoj = dataframe.seoj
        esv = dataframe.esv
        if seoj == '028801' and esv in ['72','73']:
            # 送信元が '028801'（スマートメーター）で ESV が 72（プロパティ値要求の応答）
            for epc, edt in dataframe.properties.items():
                if epc == 'E7': # 瞬時電力 E7
                    value = hex_to_signed_int(edt)
                    self.record_que.put(['BR', epc, value, 'X'])

                elif epc == 'E8': # 瞬時電流計測値
                    rphase = edt[:4]
                    tphase = edt[4:]
                    rvalue = hex_to_signed_int(rphase) * 0.1 # アンペア
                    tvalue = hex_to_signed_int(tphase) * 0.1 # アンペア
                    self.record_que.put(['BR', 'E8R', rvalue, 'X'])
                    self.record_que.put(['BR', 'E8T', tvalue, 'X'])
                    self.record_que.put(['BR', 'E8', rvalue+tvalue, 'X'])

                elif epc in ['E0','E3']: # 積算電力量（正／負）
                    value = getvalue(edt)
                    self.record_que.put(['BR', epc, value, 'X'])

                elif epc == 'D3': # 係数 coefficient D3
                    value = int(edt, 16)
                    self.coefficient = value
                    logger.debug('cofficient = ' + str(value))
                    self.record_que.put(['BR', epc, value, 'X'])

                elif epc == 'D7': # 積算電力有効桁数 effective digits D7
                    value = int(edt, 16)
                    self.effective_digits = value
                    logger.debug('effective_digits = ' + str(value))
                    self.record_que.put(['BR', epc, value, 'X'])

                elif epc == 'E1': # 積算電力単位 unit E1
                    value = int(edt, 16)
                    if value == 0x00:
                        unit = 1.0
                    elif value== 0x01:
                        unit = 0.1
                    elif value == 0x02:
                        unit = 0.01
                    elif value == 0x03:
                        unit = 0.001
                    elif value == 0x04:
                        unit = 0.0001
                    elif value == 0x0A:
                        unit = 10.0
                    elif value == 0x0B:
                        unit =  100.0
                    elif value == 0x0C:
                        unit =  1000.0
                    elif value == 0x0D:
                        unit = 10000.0
                    else:
                        value = 0.1
                    self.unit = unit
                    logger.debug('unit = ' + str(self.unit))
                    self.record_que.put(['BR','E1',value,'X'])

                elif epc in ['EA', 'EB']: # 定時 積算電力量 計測値 (正方向,逆方向計測値)
                    value = getvalue(edt[14:])
                    logger.info(datestr(edt[:14]) + ' ' + epc + ' = ' + str(value))
                    self.record_que.put(['BR', epc, value, 'X'])

                else:
                    logger.warning('unknown property:' + epc + ' value:' + edt)

        else:
            # スマートメーターからのプロパティ読み出し応答以外の電文を処理
            # ESV=73 プロパティ値通知も定期的に受信している
            #   EPC=EA:定時 積算電力量 計測値 (正方向計測値)
            #   EPC=EB:定時 積算電力量 計測値 (逆方向計測値)
            logger.warning('unknown SEOJ or ESV : ' + dataframe.seoj + ',' + dataframe.esv)
            logger.warning(dataframe.endict())

    def _term( self ):
        self.wisundev.term()
        self.wisundev.close()

    def run( self ):
        """スレッド処理"""
        logger.info('[START]')

        while not self.stopEvent.is_set():
            # スレッドループの中では、状態遷移に応じた処理を定義
            if self.state == self._STATE_INIT:
                self._open()

            elif self.state == self._STATE_OPEN:
                self._setup()

            elif self.state == self._STATE_SETUP:
                self._scan()

            elif self.state == self._STATE_SCAN:
                self._join()

            # 以下は接続状態（JOIN）においての処理
            elif self.state == self._STATE_JOIN:
                """
                ToDo:
                    トランザクション管理
                    TIDの対応付け、成功失敗時の何らかの処理したほうが良い。
                    失敗時に再送など。info の場合
                """
                now = datetime.datetime.now().timestamp()

                # 必要があればここで定期的に _rejoin() を行う。

                for req in self.requests:
                    # 要求するデータのリストについて、定期的に値要求する
                    if now - req['lasttime'] > req['cycle']:
                        cmd = DataFrame.cmd_get_property(req['epc'])
                        res = self._sendto(cmd)
                        if req['lasttime'] == 0:
                            req['lasttime'] = now
                        else:
                            req['lasttime'] += req['cycle']

                # スマートメーターからの電文を待つ
                # この関数は電文があれば直ちに DataFrame オブジェクトを返すが、
                # 受信しないとタイムアウトして空の辞書を返す
                dataframe = self._receive()

                # 受信電文処理
                if dataframe:
                    now = datetime.datetime.now().timestamp()
                    self.lasttime_receive = now
                    self._accept(dataframe)

                else:
                    # 電文受信が無かった場合
                    # print('.')
                    pass

                # 過去10分(600秒)で 電文受信が発生しなかったら初期化
                if now - self.lasttime_receive > 600:
                    logger.error('ERROR broute data receive timeout')
                    self.wisundev.term()
                    self.wisundev.close()
                    self.state = self._STATE_INIT
                    time.sleep(5)

        # スレッドストップイベントがセットされ、run()の終了の前
        self._term()
        logger.info('[STOP]')
