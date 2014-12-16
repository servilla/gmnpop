#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: pop

:Synopsis:

:Author:
  servilla
  
:Created:
  11/21/14
"""
from __future__ import print_function

__author__ = "servilla"

import sys
import pyxb
from datetime import datetime
import StringIO

import d1_client.mnclient as mnclient
import d1_client.cnclient as cnclient
import d1_client.objectlistiterator as objectlistiterator
import d1_common.types.generated.dataoneTypes as dataone_types



MN_CERT = "/Users/servilla/Certs/DataONE/urn:node:LTER.pem"
GMN_CERT = "/Users/servilla/Certs/DataONE/gmn_local.pem"
CN_BASE_URL = "https://cn.dataone.org/cn"
MN_BASE_URL = "https://metacat.lternet.edu/knb/d1/mn"
GMN_BASE_URL = "https://192.168.47.139/mn"
DATA_DIR = "./data/"


log_file = "./pop.log"
err_file = "./err.log"
pid_file = "./pid.log"

def main():

    src_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
    pid_count = 0
    pids = {}

    for metacatObjects in objectlistiterator.ObjectListIterator(src_client):
        pid = Pid(metacatObjects.identifier.value())
        pid_count += 1

        key = pid.get_key()
        series = pids.get(key)
        if series is None:
            series = [[pid.get_pid(), int(pid.get_revision())]]
        else:
            series.append([pid.get_pid(), int(pid.get_revision())])

        pids[key] = series

        if pid_count == 500: break

    key_count = 0
    for key, value in pids.iteritems():
        key_count += 1
        sorted_series = _sort_pid_series(value)
        print("(%u) %s - %s" % (key_count, key, sorted_series))

    return 0


def _get_obj(pid):

    obj = {}

    try:
        cn_client = cnclient.CoordinatingNodeClient(base_url=CN_BASE_URL, cert_path=MN_CERT)
        obj["data"] = cn_client.get(pid).read()
        obj["sys_meta_xml"] = cn_client.getSystemMetadataResponse(pid).read()
        obj["src"] = CN_BASE_URL

    except Exception as x:

        now = datetime.now().__str__()
        error_msg = now + (": OBJ GET error for (%s) on %s\n" % (pid, CN_BASE_URL)) + x.message + "\n"
        open(err_file, mode="a").write(error_msg)

        try:
            mn_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
            obj["data"] = mn_client.get(pid).read()
            obj["sys_meta_xml"] = mn_client.getSystemMetadataResponse(pid).read()
            obj["src"] = MN_BASE_URL

        except Exception as x:

            now = datetime.now().__str__()
            error_msg = now + (": OBJ GET error for (%s) on %s\n" % (pid, MN_BASE_URL)) + x.message + "\n"
            open(err_file, mode="a").write(error_msg)

            raise ObjReadException(pid)

    return obj


def _get_sys_meta(sys_meta_str):
    sys_meta_str = sys_meta_str.replace('<accessPolicy/>', '')
    sys_meta_str = sys_meta_str.replace('<blockedMemberNode/>', '')
    sys_meta_str = sys_meta_str.replace('<blockedMemberNode></blockedMemberNode>', '')

    print(sys_meta_str)

    return dataone_types.CreateFromDocument(sys_meta_str)


def _create_on_gmn(pid):

        global d1_object

        try:
            d1_object = _get_obj(pid)
            sys_meta = _get_sys_meta(d1_object["sys_meta_xml"])
            data = d1_object["data"]
            open(DATA_DIR + pid + ".dat", mode="w").write(data)
            data_file = open(DATA_DIR + pid + ".dat", mode="rb").read()

            now = datetime.now().__str__()
            log_msg = "%s: %s, formatId: %s, size: %u, src: %s\n" % (now, pid, sys_meta.formatId,
                                                                    sys_meta.size, d1_object["src"])
            open(log_file, mode="a").write(log_msg)

            gmn_client = mnclient.MemberNodeClient(base_url=GMN_BASE_URL, cert_path=GMN_CERT)
            create_response = gmn_client.create(pid, StringIO.StringIO(data_file), sys_meta)

        except ObjReadException:
            now = datetime.now().__str__()
            error_msg = "%s: %s - object not available from either CN or MN\n" % (now, pid)
            open(err_file, mode="a").write(error_msg)

        except pyxb.UnrecognizedDOMRootNodeError:
            now = datetime.now().__str__()
            error_msg = "%s: %s - pyxb parsing error\n" % (now, pid)
            open(err_file, mode="a").write(error_msg)
            open("./" + pid + ".xml", mode="w").write(d1_object["sys_meta_xml"])

        except Exception as x:
            print(x)
            now = datetime.now().__str__()
            error_msg = "%s: %s - unknown exception (%s)\n" % (now, pid, x.message)
            open(err_file, mode="a").write(error_msg)

        return 0


def _sort_pid_series(series):
    _series = sorted(series, key=lambda rev: rev[1])
    return _series


class Pid:

    def __init__(self, pid_str):
        self._pid_str = pid_str
        _pid_parts = self._pid_str.split("/")
        _pid_size = len(_pid_parts)
        self._canonical_pid = _pid_parts[_pid_size - 1]
        self._canonical_parts = self._canonical_pid.split(".")
        self._scope = self._canonical_parts[0]
        self._identifier = self._canonical_parts[1]
        self._revision = self._canonical_parts[2]

    def get_pid(self):
        return self._pid_str

    def get_canonical_pid(self):
        return self._canonical_pid

    def get_scope(self):
        return self._scope

    def get_identifier(self):
        return self._identifier

    def get_revision(self):
        return self._revision

    def get_key(self):
        return self._scope + "." + self._identifier


class ObjReadException(Exception): pass
class MNReadException(ObjReadException): pass
class CNReadException(ObjReadException): pass


if __name__ == "__main__":
    main()