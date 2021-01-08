# miczone-wrapper-php

## EventBus

### Install

In order to use this library, must be install by following https://github.com/arnaud-lb/php-rdkafka#installation

- Step 1: Install librdkafka
  https://github.com/edenhill/librdkafka
- Step 2: Install rdkafka
  https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka.installation.pecl.html

  Please choose version rdkafka-3.1.3 for high stability

### Usage

#### Notifier

```php
use Miczone\Wrapper\MiczoneEventBusClient\Notifier;

$client = new Notifier([
    'hosts' => '127.0.0.1:1234',
    'clientId' => 'my-service',
    'auth' => 'user:pass',
    'topics' => 'topicA,topicB',
    'numberOfRetries' => 3
]);

// Async
$client->ow_notifyString([
    'topic' => 'topicA',
    'key' => 'my-key', // to keep packages in order if they have the same key
    'data' => '{}' // data type can be Boolean / Integer / Double / String
], function () {
    // Success callback
}, function ($ex) {
    // Error callback
});

// Sync
$response = $client->notifyString([
    'topic' => 'topicB',
    'key' => 'my-key',
    'data' => '{}',
]);
```

#### Watcher

```php
use Miczone\Wrapper\MiczoneEventBusClient\Watcher;

$client = new Watcher([
    'hosts' => '127.0.0.1:1234',
    'clientId' => 'my-service',
    'groupId' => 'my-group',
    'topics' => 'topicA,topicB'
]);

// Handler type can be BooleanWatcherHandlerInterface / IntegerWatcherHandlerInterface, DoubleWatcherHandlerInterface, StringWatcherHandlerInterface
$client->setHandler(new class implements StringWatcherHandlerInterface {
    public function onMessage(string $topic, int $partition, int $offset, int $timestamp, string $key = null, string $data = null) {
        // Do your stuff
    }

    public function onError($code, string $message, \Exception $exception = null) {
        // Catch error
    }
});

$client->start();
```
