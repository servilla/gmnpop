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

import pyxb
import hashlib
import StringIO
from datetime import datetime
import ssl


import d1_client.mnclient as mnclient
import d1_client.cnclient as cnclient
import d1_client.objectlistiterator as objectlistiterator
import d1_common.types.generated.dataoneTypes as dataoneTypes
from pidgeon import Pidgeon
import properties

MN_CERT = properties.MN_CERT
GMN_CERT = properties.GMN_CERT
GMN_KEY = properties.GMN_KEY
CN_BASE_URL = properties.CN_BASE_URL
MN_BASE_URL = properties.MN_BASE_URL
GMN_BASE_URL = properties.GMN_BASE_URL
DATA_DIR = properties.DATA_DIR

log_file = "./pop.log"
err_file = "./err.log"
pid_file = "./pid.log"

def main():

    # This restores the same SSL behavior as Python 2.7.8 and earlier - bad.
    ssl._create_default_https_context = ssl._create_unverified_context

    src_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
    pids = _get_ordered_pid_list(src_client, max_pids=500)

    key_count = 0

    for pid_base, series in pids.iteritems():
        key_count += 1
        open(log_file, mode="a").write("(%u) %s - %s\n" % (key_count, pid_base, series))

        pid_count = 0
        d1_pid_old = None

        for pid in series:

            pid_count += 1
            pidgeon = Pidgeon(pid[0])
            d1_pid = pidgeon.get_d1_pid()

            # Get object from CN if available; otherwise, from MN
            try:
                cn_client = cnclient.CoordinatingNodeClient(base_url=CN_BASE_URL, cert_path=MN_CERT)
                sys_meta = _get_sys_meta(d1_pid, cn_client)
                obj = cn_client.get(d1_pid).read()
            except Exception as e:
                now = datetime.now().__str__()
                error_msg = now + (": READ error for %s at %s\n" % (d1_pid, CN_BASE_URL)) + e.message + "\n"
                open(err_file, mode="a").write(error_msg)
                try:
                    mn_client = mnclient.MemberNodeClient(base_url=MN_BASE_URL, cert_path=MN_CERT)
                    sys_meta = _get_sys_meta(d1_pid, mn_client)
                    obj = mn_client.get(d1_pid).read()
                except Exception as e:
                    now = datetime.now().__str__()
                    error_msg = now + (": READ error for %s at %s\n" % (d1_pid, MN_BASE_URL)) + e.message + "\n"
                    open(err_file, mode="a").write(error_msg)
                    break

            open(DATA_DIR + pidgeon.get_knb_pid() + ".dat", mode="w").write(obj)
            obj = open(DATA_DIR + pidgeon.get_knb_pid() + ".dat", mode="rb").read()

            gmn_sys_meta = _gen_sys_meta(sys_meta, obj)

            #print(mn_sys_meta.toxml("utf-8"))
            #print(gmn_sys_meta.toxml("utf-8"))
            #print(cn_sys_meta.toxml("utf-8"))

            gmn_client = mnclient.MemberNodeClient(base_url=GMN_BASE_URL, cert_path=GMN_CERT, key_path=GMN_KEY)

            if pid_count == 1:
                try:
                    open(log_file, mode="a").write("CREATE: %s\n" % d1_pid)
                    #print(gmn_sys_meta.toxml("utf-8"))
                    create_response = gmn_client.create(d1_pid, StringIO.StringIO(obj), gmn_sys_meta)
                    d1_pid_old = d1_pid
                except Exception as e:
                    now = datetime.now().__str__()
                    error_msg = now + (": OBJ CREATE error for %s at %s\n" % (d1_pid, GMN_BASE_URL)) + e.message + "\n"
                    open(err_file, mode="a").write(error_msg)
                    break
            else:
                try:
                    open(log_file, mode="a").write("UPDATE: %s -> %s\n" % (d1_pid_old, d1_pid))
                    #print(gmn_sys_meta.toxml("utf-8"))
                    update_response = gmn_client.update(d1_pid_old, StringIO.StringIO(obj), d1_pid, gmn_sys_meta)
                    d1_pid_old = d1_pid
                except Exception as e:
                    now = datetime.now().__str__()
                    error_msg = now + (": OBJ UPDATE error for %s at %s\n" % (d1_pid, GMN_BASE_URL)) + e.message + "\n"
                    open(err_file, mode="a").write(error_msg)
                    break

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
        _sys_meta_obj = dataoneTypes.CreateFromDocument(sys_meta_str)
        return _sys_meta_obj
    except pyxb.UnrecognizedDOMRootNodeError as e:
        now = datetime.now().__str__()
        error_msg = "%s: %s - pyxb parsing error: %s\n" % (now, d1_pid, e.message)
        pidgeon = Pidgeon(d1_pid)
        open(err_file, mode="a").write(error_msg)
        open("./" + pidgeon.get_knb_pid() + ".sysmeta.xml", mode="w").write(sys_meta_str)
        raise SysMetaReadException(e.message)
    except Exception as e:
        raise SysMetaReadException(e.message)


def _get_ordered_pid_list(src_client, max_pids=None, named_pid=None):
    """Return ordered list of all source client pids

    :param src_client:
    The src_client DataONE object - see d1_client.mnclient

    :param max_pids=None:
    Maximum number of pids to process

    :param single_pid=None:
    Operate on a single client provided pid

    :return:
    Dictionary of package series representing pid obsolescence chains
    """

    pid_count = 0
    pids = {}

    # Build dictionary of package identifiers with revisions in non-sorted list; each
    # entry ("scope.identifier": "revX", "revY", ...) represents a data package series

    if named_pid is None:
        # Iterate over full corpus of objects
        for metacatObjects in objectlistiterator.ObjectListIterator(src_client):
            pid = Pidgeon(metacatObjects.identifier.value())
            pid_count += 1

            pid_msg = "%d: %s\n" % (pid_count,pid)
            open(pid_file, mode="a").write(pid_msg)

            key = pid.get_key()
            series = pids.get(key)
            if series is None:
                series = [[pid.get_d1_pid(), int(pid.get_revision())]]
            else:
                series.append([pid.get_d1_pid(), int(pid.get_revision())])

            pids[key] = series

            if max_pids is not None:
                if pid_count == max_pids: break
    else:
        pid = Pidgeon(named_pid)
        series = [[pid.get_d1_pid(), int(pid.get_revision())]]
        key = pid.get_key()
        pids[key] = series

    key_count = 0

    # Iterate through dictionary and sort list of revisions to create ordered obsolescence
    # chain
    for key, value in pids.iteritems():
        key_count += 1
        pids[key] = sorted(value, key=lambda rev: rev[1])

    return pids


def _gen_sys_meta(sys_meta, obj):
    '''Return system metadata for GMN

    :param sys_meta:
    Source system metadata from either CN or MN

    :param obj:
    Science object

    :return:
    GMN system metadata
    '''

    # Compute new values for size and hash
    size = len(obj)
    md5 = hashlib.md5(obj).hexdigest()

    # Merge desired system metadata from MN or CN into GMN
    _sys_meta = dataoneTypes.systemMetadata()
    _sys_meta.identifier = sys_meta.identifier
    _sys_meta.formatId = sys_meta.formatId
    _sys_meta.size = size
    _sys_meta.rightsHolder = sys_meta.rightsHolder
    _sys_meta.checksum = dataoneTypes.checksum(md5)
    _sys_meta.checksum.algorithm = "MD5"
    _sys_meta.dateUploaded = sys_meta.dateUploaded
    _sys_meta.dateSysMetadataModified = sys_meta.dateSysMetadataModified
    _sys_meta.accessPolicy = sys_meta.accessPolicy

    # Only for test environments
    _sys_meta.authoritativeMemberNode = "Nemo GMN localClient"

    return _sys_meta


class ObjReadException(Exception): pass
class SysMetaReadException(Exception): pass


if __name__ == "__main__":
    main()