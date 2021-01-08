<?php
namespace Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler;

interface StringWatcherHandlerInterface {
  public function onMessage(string $topic, int $partition, int $offset, int $timestamp, string $key = null, string $data = null);

  public function onError($code, string $message, \Exception $exception = null);
}
