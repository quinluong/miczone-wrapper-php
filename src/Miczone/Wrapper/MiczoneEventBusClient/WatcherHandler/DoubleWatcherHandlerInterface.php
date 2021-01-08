<?php
namespace Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler;

interface DoubleWatcherHandlerInterface {
  public function onMessage(string $topic, int $partition, int $offset, int $timestamp, string $key = null, float $data = null);

  public function onError($code, string $message, \Exception $exception = null);
}
