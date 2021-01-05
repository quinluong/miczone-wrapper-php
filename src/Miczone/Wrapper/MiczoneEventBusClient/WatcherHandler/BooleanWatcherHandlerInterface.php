<?php
namespace Miczone\Wrapper\MiczoneEventBusClient\WatcherHandler;

interface BooleanWatcherHandlerInterface {
  public function onMessage(string $topic, int $partition, int $offset, string $key = null, bool $data = null);

  public function onError($code, string $message, \Exception $exception = null);
}
