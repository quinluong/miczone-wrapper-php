<?php

namespace Miczone\Wrapper;

class MiczoneClientBase {

  protected function standardizeHosts(string $hosts) {
    if (empty($hosts)) {
      return [];
    }

    $hosts = explode(',', $hosts);

    if (empty($hosts)) {
      return [];
    }

    $hostPorts = [];

    foreach ($hosts as $item) {
      $hostPortPair = explode(':', $item);

      if (count($hostPortPair) !== 2) {
        continue;
      }

      $host = trim($hostPortPair[0]);
      $port = (int) trim($hostPortPair[1]);

      if (empty($host) || $port <= 0) {
        continue;
      }

      \array_push($hostPorts, [
        'host' => $host,
        'port' => $port,
      ]);
    }

    return $hostPorts;
  }

  protected function initHostPortsAliveStatus(array $hostPorts) {
    if (empty($hostPorts)) {
      return [];
    }

    $result = [];

    foreach ($hostPorts as $hostPort) {
      $key = $hostPort['host'] . ':' . $hostPort['port'];

      $result[$key] = true;
    }

    return $result;
  }

  protected function markHostPortAlive(array &$hostPortsAliveStatus, array $hostPort) {
    if (empty($hostPortsAliveStatus) || empty($hostPort)) {
      return;
    }

    $key = $hostPort['host'] . ':' . $hostPort['port'];

    $hostPortsAliveStatus[$key] = true;
  }

  protected function markHostPortDead(array &$hostPortsAliveStatus, array $hostPort) {
    if (empty($hostPortsAliveStatus) || empty($hostPort)) {
      return;
    }

    $key = $hostPort['host'] . ':' . $hostPort['port'];

    $hostPortsAliveStatus[$key] = false;
  }

  protected function getHostPortAliveStatus(array $hostPortsAliveStatus, array $hostPort) {
    if (empty($hostPortsAliveStatus) || empty($hostPort)) {
      return false;
    }

    $key = $hostPort['host'] . ':' . $hostPort['port'];

    if (!isset($hostPortsAliveStatus[$key])) {
      return false;
    }

    return $hostPortsAliveStatus[$key];
  }

  protected function standardizeAuth(string $auth) {
    if (empty($auth)) {
      return [];
    }

    $auth = explode(':', $auth);

    if (count($auth) !== 2) {
      return [];
    }

    $username = trim($auth[0]);
    $password = trim($auth[1]);

    if (empty($username) || empty($password)) {
      return [];
    }

    $result = [
      'username' => $username,
      'password' => $password,
    ];

    return $result;
  }

  protected function standardizeTopics(string $topics) {
    if (empty($topics)) {
      return [];
    }

    $topics = explode(',', $topics);

    if (empty($topics)) {
      return [];
    }

    $result = [];

    foreach ($topics as $topic) {
      $topic = trim($topic);

      if (empty($topic)) {
        continue;
      }

      \array_push($result, $topic);
    }

    return $result;
  }

}
