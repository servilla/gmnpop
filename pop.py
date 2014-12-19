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
            _create_on_gmn(Pidgeon(pid[0]))

    return 0


def _get_obj(d1_pid):
    """Return a DataONE object consisting of both data and system metadata

    :param pid:
    The DataONE pid string

    :return:
    Object bytes, system metadata XML, and source URL as a dictionary
    """

    obj = {}

    # Try to get object from CN first, then MN; otherwise, raise ObjReadException
    try:
        cn_client = cnclient.CoordinatingNodeClient(base_url=CN_BASE_URL, cert_path=MN_CERT)
        obj["data"] = cn_client.get(d1_pid).read()
        obj["sys_meta_xml"] = cn_client.getSystemMetadataResponse(d1_pid).read()
        obj["src"] = CN_BASE_URL

    except Exception as x:

        now = datetime.now().__str__()
        error_msg = now + (": OBJ GET error for (%s) on %s\n" % (d1_pid, CN_BASE_URL)) + x.message + "\n"
        open(err_file, mode="a").write(error_msg)

        try:
            mn_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
            obj["data"] = mn_client.get(d1_pid).read()
            obj["sys_meta_xml"] = mn_client.getSystemMetadataResponse(d1_pid).read()
            obj["src"] = MN_BASE_URL

        except Exception as x:

            now = datetime.now().__str__()
            error_msg = now + (": OBJ GET error for (%s) on %s\n" % (d1_pid, MN_BASE_URL)) + x.message + "\n"
            open(err_file, mode="a").write(error_msg)

            raise ObjReadException(d1_pid)

    return obj


def _get_sys_meta(sys_meta_str):
    """Return corrected system metadata as pyxb object
    :param sys_meta_str:
    Raw system metadata XML string

    :return:
    System metadata as pyxb object
    """

    sys_meta_str = sys_meta_str.replace('<accessPolicy/>', '')
    sys_meta_str = sys_meta_str.replace('<blockedMemberNode/>', '')
    sys_meta_str = sys_meta_str.replace('<blockedMemberNode></blockedMemberNode>', '')

    _sys_meta_obj = dataone_types.CreateFromDocument(sys_meta_str)

    return _sys_meta_obj


def _create_on_gmn(pid):
    """Create a object on the GMN

    :param d1_pid:
    The DataONE pid

    :return:
    Success status
    """

    d1_object = None

    try:
        d1_object = _get_obj(pid.get_d1_pid())
        sys_meta = _get_sys_meta(d1_object["sys_meta_xml"])
        data = d1_object["data"]
        open(DATA_DIR + pid + ".dat", mode="w").write(data)
        data_file = open(DATA_DIR + pid + ".dat", mode="rb").read()

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
class MNReadException(ObjReadException): pass
class CNReadException(ObjReadException): pass


if __name__ == "__main__":
    main()