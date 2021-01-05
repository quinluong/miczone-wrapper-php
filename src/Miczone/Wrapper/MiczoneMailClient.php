<?php

namespace Miczone\Wrapper;

use Miczone\Thrift\Common\Error;
use Miczone\Thrift\Common\ErrorCode;
use Miczone\Thrift\Common\OperationHandle;
use Miczone\Thrift\Mail\CallbackUrl;
use Miczone\Thrift\Mail\ContentType;
use Miczone\Thrift\Mail\EmailNamePair;
use Miczone\Thrift\Mail\FromWebsite;
use Miczone\Thrift\Mail\GetMailInfoRequest;
use Miczone\Thrift\Mail\GetMailInfoResponse;
use Miczone\Thrift\Mail\MailInfo;
use Miczone\Thrift\Mail\MiczoneMailServiceClient;
use Miczone\Thrift\Mail\SendMailRequest;
use Miczone\Thrift\Mail\SendMailResponse;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocket;

class MiczoneMailClient {
  const CLIENT_VERSION = 'v1.0';

  const HOSTS = [];

  const AUTH = '';

  const SEND_TIMEOUT_IN_MILLISECONDS = 5000;

  const RECEIVE_TIMEOUT_IN_MILLISECONDS = 5000;

  const NUMBER_OF_RETRIES = 3;

  protected $config;

  protected $operationHandle;

  protected $lastException;

  public function __construct(array $config = []) {
    $config = array_merge([
      'hosts' => static::HOSTS,
      'auth' => static::AUTH,
      'sendTimeoutInMilliseconds' => static::SEND_TIMEOUT_IN_MILLISECONDS,
      'receiveTimeoutInMilliseconds' => static::RECEIVE_TIMEOUT_IN_MILLISECONDS,
      'numberOfRetries' => static::NUMBER_OF_RETRIES,
    ], $config);

    $config['hosts'] = $this->_standardizeHosts($config['hosts']);

    if (empty($config['hosts'])) {
      throw new \Exception('Invalid "hosts" config');
    }

    $config['auth'] = $this->_standardizeAuth($config['auth']);

    if (empty($config['auth'])) {
      throw new \Exception('Invalid "auth" config');
    }

    if (!is_int($config['sendTimeoutInMilliseconds']) || $config['sendTimeoutInMilliseconds'] <= 0) {
      $config['sendTimeoutInMilliseconds'] = static::SEND_TIMEOUT_IN_MILLISECONDS;
    }

    if (!is_int($config['receiveTimeoutInMilliseconds']) || $config['receiveTimeoutInMilliseconds'] <= 0) {
      $config['receiveTimeoutInMilliseconds'] = static::RECEIVE_TIMEOUT_IN_MILLISECONDS;
    }

    if (!is_int($config['numberOfRetries']) || $config['numberOfRetries'] <= 0) {
      $config['numberOfRetries'] = static::NUMBER_OF_RETRIES;
    }

    $this->config = $config;

    $this->operationHandle = new OperationHandle([
      'username' => $this->config['auth']['username'],
      'password' => $this->config['auth']['password'],
    ]);
  }

  public function __destruct() {
  }

  private function _standardizeHosts(string $hosts) {
    if (empty($hosts)) {
      return [];
    }

    $hosts = explode(',', $hosts);

    if (empty($hosts)) {
      return [];
    }

    $result = [];

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
      array_push($result, [
        'host' => $host,
        'port' => $port,
      ]);
    }

    return $result;
  }

  private function _standardizeAuth(string $auth) {
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

  private function _createSendMailRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_int($params['fromWebsite']) || !array_key_exists($params['fromWebsite'], FromWebsite::$__names)) {
      throw new \Exception('Invalid "fromWebsite" param');
    }

    if (!is_int($params['contentType']) || !array_key_exists($params['contentType'], ContentType::$__names)) {
      throw new \Exception('Invalid "contentType" param');
    }

    if (!is_string($params['fromEmail']) || empty($params['fromEmail'])) {
      throw new \Exception('Invalid "fromEmail" param');
    }

    if (!empty($params['fromName']) && !is_string($params['fromName'])) {
      throw new \Exception('Invalid "fromName" param');
    }

    $replyToList = [];
    if (!empty($params['replyToList']) && is_array($params['replyToList'])) {
      foreach ($params['replyToList'] as $item) {
        if (empty($item) || !is_array($item) || empty($item['email'])) {
          continue;
        }
        $emailNamePair = new EmailNamePair();
        $emailNamePair->email = $item['email'];
        if (!empty($item['name'])) {
          $emailNamePair->name = $item['name'];
        }
        array_push($replyToList, $emailNamePair);
      }
    }

    if (empty($params['toList']) || !is_array($params['toList'])) {
      throw new \Exception('Invalid "toList" param');
    }

    $toList = [];
    foreach ($params['toList'] as $item) {
      if (empty($item) || !is_array($item) || empty($item['email'])) {
        continue;
      }
      $emailNamePair = new EmailNamePair();
      $emailNamePair->email = $item['email'];
      if (!empty($item['name'])) {
        $emailNamePair->name = $item['name'];
      }
      array_push($toList, $emailNamePair);
    }

    if (empty($toList)) {
      throw new \Exception('Invalid "toList" param');
    }

    $ccList = [];
    if (!empty($params['ccList']) && is_array($params['ccList'])) {
      foreach ($params['ccList'] as $item) {
        if (empty($item) || !is_array($item) || empty($item['email'])) {
          continue;
        }
        $emailNamePair = new EmailNamePair();
        $emailNamePair->email = $item['email'];
        if (!empty($item['name'])) {
          $emailNamePair->name = $item['name'];
        }
        array_push($ccList, $emailNamePair);
      }
    }

    $bccList = [];
    if (!empty($params['bccList']) && is_array($params['bccList'])) {
      foreach ($params['bccList'] as $item) {
        if (empty($item) || !is_array($item) || empty($item['email'])) {
          continue;
        }
        $emailNamePair = new EmailNamePair();
        $emailNamePair->email = $item['email'];
        if (!empty($item['name'])) {
          $emailNamePair->name = $item['name'];
        }
        array_push($bccList, $emailNamePair);
      }
    }

    if (!empty($params['bounceEmail']) && !is_string($params['bounceEmail'])) {
      throw new \Exception('Invalid "bounceEmail" param');
    }

    if (!is_string($params['subject']) || empty($params['subject'])) {
      throw new \Exception('Invalid "subject" param');
    }

    if (!is_string($params['content']) || empty($params['content'])) {
      throw new \Exception('Invalid "content" param');
    }

    if (!empty($params['successCallbackUrlList']) && !is_array($params['successCallbackUrlList'])) {
      throw new \Exception('Invalid "successCallbackUrlList" param');
    }

    if (!empty($params['errorCallbackUrlList']) && !is_array($params['errorCallbackUrlList'])) {
      throw new \Exception('Invalid "errorCallbackUrlList" param');
    }

    $mailInfo = new MailInfo();
    $mailInfo->fromWebsite = $params['fromWebsite'];
    $mailInfo->contentType = $params['contentType'];
    $mailInfo->fromEmail = $params['fromEmail'];
    if (!empty($params['fromName'])) {
      $mailInfo->fromName = $params['fromName'];
    }
    if (!empty($replyToList)) {
      $mailInfo->replyToList = $replyToList;
    }
    $mailInfo->toList = $toList;
    if (!empty($ccList)) {
      $mailInfo->ccList = $ccList;
    }
    if (!empty($bccList)) {
      $mailInfo->bccList = $bccList;
    }
    if (!empty($params['bounceEmail'])) {
      $mailInfo->bounceEmail = $params['bounceEmail'];
    }
    $mailInfo->subject = $params['subject'];
    $mailInfo->content = $params['content'];

    $callbackUrl = new CallbackUrl();
    if (!empty($params['successCallbackUrlList'])) {
      $callbackUrl->successList = $params['successCallbackUrlList'];
    }
    if (!empty($params['errorCallbackUrlList'])) {
      $callbackUrl->errorList = $params['errorCallbackUrlList'];
    }

    $request = new SendMailRequest([
      'mailInfo' => $mailInfo,
      'callbackUrl' => $callbackUrl,
    ]);

    return $request;
  }

  private function _createGetMailInfoRequest(array $params = []) {
    if (empty($params)) {
      throw new \Exception('Invalid array params');
    }

    if (!is_int($params['fromWebsite']) || !array_key_exists($params['fromWebsite'], FromWebsite::$__names)) {
      throw new \Exception('Invalid "fromWebsite" param');
    }

    if (!is_int($params['partition']) || $params['partition'] < 0) {
      throw new \Exception('Invalid "partition" param');
    }

    if (!is_int($params['offset']) || $params['offset'] < 0) {
      throw new \Exception('Invalid "offset" param');
    }

    $request = new GetMailInfoRequest([
      'fromWebsite' => $params['fromWebsite'],
      'partition' => $params['partition'],
      'offset' => $params['offset'],
    ]);

    return $request;
  }

  private function _createTransportAndClient($host, $port) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($this->config['receiveTimeoutInMilliseconds']);
    $socket->setRecvTimeout($this->config['sendTimeoutInMilliseconds']);

    $transport = new TFramedTransport($socket);

    $protocol = new TBinaryProtocol($transport);

    $client = new MiczoneMailServiceClient($protocol);

    return [$transport, $client];
  }

  public function getLastException() {
    return $this->lastException;
  }

  /**
   * @return \Miczone\Thrift\Common\ErrorCode
   */
  public function ping() {
    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->ping($this->operationHandle);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return ErrorCode::THRIFT_BAD_REQUEST;
  }

  /**
   * @param array $params Contains:
   * int      fromWebsite (required) // ! refer to enum \Miczone\Thrift\Mail\FromWebsite
   * int      contentType (required) // ! refer to enum \Miczone\Thrift\Mail\ContentType
   * string   fromEmail (required)
   * string   fromName
   * array    replyToList // ! each item has structure: [ 'email' => 'mail@domain.com', 'name' => 'Email Name' ]
   * array    toList (required) // ! each item has structure: [ 'email' => 'mail@domain.com', 'name' => 'Email Name' ]
   * array    ccList // ! each item has structure: [ 'email' => 'mail@domain.com', 'name' => 'Email Name' ]
   * array    bccList // ! each item has structure: [ 'email' => 'mail@domain.com', 'name' => 'Email Name' ]
   * string   bounceEmail
   * string   subject (required)
   * string   content (required)
   * array    successCallbackUrlList
   * array    errorCallbackUrlList
   * @return \Miczone\Thrift\Mail\SendMailResponse
   * @throws \Exception
   */
  public function send(array $params = []) {
    $request = $this->_createSendMailRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->send($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new SendMailResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

  /**
   * @param array $params Contains:
   * int      fromWebsite (required) // ! refer to enum \Miczone\Thrift\Mail\FromWebsite
   * int      contentType (required) // ! refer to enum \Miczone\Thrift\Mail\ContentType
   * string   fromEmail (required)
   * string   fromName
   * array    replyToList // ! each item has structure: [ 'email' => 'mail@domain.com', 'name' => 'Email Name' ]
   * array    toList (required) // ! each item has structure: [ 'email' => 'mail@domain.com', 'name' => 'Email Name' ]
   * string   bounceEmail
   * string   subject (required)
   * string   content (required)
   * array    successCallbackUrlList
   * array    errorCallbackUrlList
   * @throws \Exception
   */
  public function ow_send(array $params = [], callable $successCallback = null, callable $errorCallback = null) {
    $request = $this->_createSendMailRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $client->ow_send($this->operationHandle, $request);
          $transport->close();

          if (is_callable($successCallback)) {
            call_user_func($successCallback);
          }

          return;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    if (is_callable($errorCallback)) {
      call_user_func($errorCallback, $this->lastException);
    }
  }

  /**
   * @param array $params Contains:
   * int      fromWebsite (required) // ! refer to enum \Miczone\Thrift\Mail\FromWebsite
   * int      partition (required)
   * int      offset (required)
   * @return \Miczone\Thrift\Mail\GetMailInfoResponse
   * @throws \Exception
   */
  public function getInfo(array $params = []) {
    $request = $this->_createGetMailInfoRequest($params);

    foreach ($this->config['hosts'] as $hostPortPair) {
      for ($i = 0; $i < $this->config['numberOfRetries']; $i++) {
        list($transport, $client) = $this->_createTransportAndClient($hostPortPair['host'], $hostPortPair['port']);

        if ($client === null) {
          // Do something ...
          break;
        }

        try {
          $transport->open();
          $result = $client->getInfo($this->operationHandle, $request);
          $transport->close();

          return $result;
        } catch (TTransportException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (TException $ex) {
          $this->lastException = $ex;
          // Do something ...
        } catch (\Exception $ex) {
          $this->lastException = $ex;
          // Do something ...
        }
      }
    }

    return new GetMailInfoResponse([
      'error' => new Error([
        'code' => ErrorCode::THRIFT_BAD_REQUEST,
      ]),
    ]);
  }

}
