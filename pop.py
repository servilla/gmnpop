#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: pop.py

:Synopsis:
    To populate PASTA GMN with legacy LTER Metacat DataONE MN objects.

:Author:
    servilla
  
:Created:
    11/21/14
"""
from __future__ import print_function

__author__ = "servilla"

import sys
import pyxb
import StringIO
from datetime import datetime

import d1_client.mnclient as mnclient
import d1_client.cnclient as cnclient
import d1_client.objectlistiterator as objectlistiterator
import d1_common.types.generated.dataoneTypes as dataone_types

from pidgeon import Pidgeon

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
    pids = _get_ordered_pid_list(src_client,10)

    key_count = 0

    for pid_base, series in pids.iteritems():
        key_count += 1
        print("(%u) %s - %s" % (key_count, pid_base, series))

        for pid in series:

            pidgeon = Pidgeon(pid[0])
            d1_pid = pidgeon.get_d1_pid()

            # Get object system metadata from MN
            try:
                mn_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
                mn_sys_meta = _get_sys_meta(d1_pid, mn_client)
            except SysMetaReadException as e:
                now = datetime.now().__str__()
                error_msg = now + (": SYS_META READ error for %s at %s\n" % (d1_pid, MN_BASE_URL)) + e.message + "\n"
                open(err_file, mode="a").write(error_msg)
                break

            # Get object system metadata from CN
            try:
                cn_client = cnclient.CoordinatingNodeClient(base_url=CN_BASE_URL, cert_path=MN_CERT)
                cn_sys_meta = _get_sys_meta(d1_pid, cn_client)
            except SysMetaReadException as e:
                now = datetime.now().__str__()
                error_msg = now + (": SYS_META READ error for %s at %s\n" % (d1_pid, CN_BASE_URL)) + e.message + "\n"
                open(err_file, mode="a").write(error_msg)
                break

            # Get object from CN if available; otherwise, from MN
            try:
                cn_client = cnclient.CoordinatingNodeClient(base_url=CN_BASE_URL, cert_path=MN_CERT)
                obj = cn_client.get(d1_pid).read()
            except Exception as e:
                now = datetime.now().__str__()
                error_msg = now + (": OBJ READ error for %s at %s\n" % (d1_pid, CN_BASE_URL)) + e.message + "\n"
                open(err_file, mode="a").write(error_msg)
                try:
                    mn_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
                    obj = mn_client.get(d1_pid).read()
                except Exception as e:
                    now = datetime.now().__str__()
                    error_msg = now + (": OBJ READ error for %s at %s\n" % (d1_pid, MN_BASE_URL)) + e.message + "\n"
                    open(err_file, mode="a").write(error_msg)
                    break

            open(DATA_DIR + pidgeon.get_knb_pid() + ".dat", mode="w").write(obj)
            obj = open(DATA_DIR + pidgeon.get_knb_pid() + ".dat", mode="rb").read()


    return 0


def _get_sys_meta(d1_pid, client):
    """Return corrected system metadata as pyxb object

    :param d1_pid:
    The DataONE pid string

    :param client:
    Either the DataONE Member Node or Coordinating Node Client object

    :return:
    System metadata as pyxb object
    """

    try:
        sys_meta_str = client.getSystemMetadataResponse(d1_pid).read()
        sys_meta_str = sys_meta_str.replace('<accessPolicy/>', '')
        sys_meta_str = sys_meta_str.replace('<blockedMemberNode/>', '')
        sys_meta_str = sys_meta_str.replace('<blockedMemberNode></blockedMemberNode>', '')
        _sys_meta_obj = dataone_types.CreateFromDocument(sys_meta_str)
        return _sys_meta_obj
    except pyxb.UnrecognizedDOMRootNodeError as e:
        now = datetime.now().__str__()
        error_msg = "%s: %s - pyxb parsing error: %s\n" % (now, d1_pid, e.message)
        open(err_file, mode="a").write(error_msg)
        open("./" + d1_pid + ".xml", mode="w").write(sys_meta_str)
        raise SysMetaReadException(e.message)
    except Exception as e:
        raise SysMetaReadException(e.message)


def _create_on_gmn(pid):
    """Create a object on the GMN

    :param d1_pid:
    The DataONE pid string

    :return:
    Success status
    """

    d1_object = None

    try:
        d1_object = _get_obj(pid.get_d1_pid())
        sys_meta = _get_sys_meta(d1_object["sys_meta_xml"])
        data = d1_object["data"]
        open(DATA_DIR + pid.get_knb_pid() + ".dat", mode="w").write(data)
        data_file = open(DATA_DIR + pid.get_knb_pid() + ".dat", mode="rb").read()

        now = datetime.now().__str__()
        log_msg = "%s: %s, formatId: %s, size: %u, src: %s\n" % (now, pid, sys_meta.formatId,
                                                                 sys_meta.size, d1_object["src"])
        open(log_file, mode="a").write(log_msg)

        #gmn_client = mnclient.MemberNodeClient(base_url=GMN_BASE_URL, cert_path=GMN_CERT)
        #create_response = gmn_client.create(pid, StringIO.StringIO(data_file), sys_meta)

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


def _get_ordered_pid_list(src_client, max_pids=None):
    """Return ordered list of all source client pids

    :param src_client:
    The src_client DataONE object - see d1_client.mnclient

    :param max_pids=None:
    Maximum number of pids to process

    :return:
    Dictionary of package series representing pid obsolescence chains
    """

    pid_count = 0
    pids = {}

    # Build dictionary of package identifiers with revisions in non-sorted list; each
    # entry ("scope.identifier": "revX", "revY", ...) represents a data package series
    for metacatObjects in objectlistiterator.ObjectListIterator(src_client):
        pid = Pidgeon(metacatObjects.identifier.value())
        pid_count += 1

        key = pid.get_key()
        series = pids.get(key)
        if series is None:
            series = [[pid.get_d1_pid(), int(pid.get_revision())]]
        else:
            series.append([pid.get_d1_pid(), int(pid.get_revision())])

        pids[key] = series

        if max_pids is not None:
            if pid_count == max_pids: break

    key_count = 0

    # Iterate through dictionary and sort list of revisions to create ordered obsolescence
    # chain
    for key, value in pids.iteritems():
        key_count += 1
        pids[key] = sorted(value, key=lambda rev: rev[1])

        #print("(%u) %s - %s" % (key_count, key, sorted_series))

    return pids


class ObjReadException(Exception): pass
class SysMetaReadException(Exception): pass


if __name__ == "__main__":
    main()