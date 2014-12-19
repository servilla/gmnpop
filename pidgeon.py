#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: pidgeon.py

:Synopsis:
    Persistent Identifier Generic Editor Object Notation for managing DataONE and KNB
    identifier notation

:Author:
    servilla
  
:Created:
    12/18/14
"""

__author__ = "servilla"

class Pidgeon:

    def __init__(self, pid_str):
        self._pid_str = pid_str
        _pid_parts = self._pid_str.split("/")
        _pid_size = len(_pid_parts)
        self._knb_pid = _pid_parts[_pid_size - 1]
        self._knb_parts = self._knb_pid.split(".")
        self._scope = self._knb_parts[0]
        self._identifier = self._knb_parts[1]
        self._revision = self._knb_parts[2]

    def get_d1_pid(self):
        return self._pid_str

    def get_knb_pid(self):
        return self._knb_pid

    def get_scope(self):
        return self._scope

    def get_identifier(self):
        return self._identifier

    def get_revision(self):
        return self._revision

    def get_key(self):
        return self._scope + "." + self._identifier
