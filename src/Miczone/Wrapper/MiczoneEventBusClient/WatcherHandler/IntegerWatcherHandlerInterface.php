<?php
namespace Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler;

interface IntegerWatcherHandlerInterface {
  public function onMessage(string $topic, int $partition, int $offset, int $timestamp, string $key = null, int $data = null);

  public function onError($code, string $message, \Exception $exception = null);
}
