<?php

declare(strict_types=1);

namespace Tests\Unit;

use Illuminate\Contracts\Bus\Dispatcher;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Database\QueryException;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Facades\Schema;
use RuntimeException;
use Tests\Fixtures\TestSimpleWorkflow;
use Tests\TestCase;
use Workflow\Models\StoredWorkflow;
use Workflow\Serializers\Serializer;
use Workflow\States\WorkflowPendingStatus;
use Workflow\Watchdog;

final class WatchdogDatabaseCacheTest extends TestCase
{
    private const PREFIX = 'watchdog-test:';

    protected function setUp(): void
    {
        parent::setUp();

        config([
            'database.connections.watchdog-cache' => config('database.connections.' . config('database.default')),
            'cache.default' => 'watchdog',
            'cache.stores.watchdog' => [
                'driver' => 'database',
                'connection' => 'watchdog-cache',
                'table' => 'watchdog_cache',
                'lock_table' => 'watchdog_locks',
                'lock_lottery' => [0, 100],
                'prefix' => self::PREFIX,
            ],
        ]);

        Schema::connection('watchdog-cache')->dropIfExists('watchdog_cache');
        Schema::connection('watchdog-cache')->dropIfExists('watchdog_locks');
        Schema::connection('watchdog-cache')->create('watchdog_cache', static function (Blueprint $table): void {
            $table->string('key')
                ->primary();
            $table->text('value');
            $table->integer('expiration');
        });
        Schema::connection('watchdog-cache')->create('watchdog_locks', static function (Blueprint $table): void {
            $table->string('key')
                ->primary();
            $table->string('owner');
            $table->integer('expiration');
        });

        Queue::fake();
        $this->freezeTime();
        DB::connection('watchdog-cache')->enableQueryLog();
    }

    public function testActiveChainDoesNotAttemptTheChainLock(): void
    {
        Cache::put('workflow:watchdog', 'active', 360);
        DB::connection('watchdog-cache')->flushQueryLog();

        for ($i = 0; $i < 20; $i++) {
            Watchdog::wake('redis');
        }

        Queue::assertNothingPushed();
        $this->assertSame('active', Cache::get('workflow:watchdog'));
        $this->assertChainLockAttempts(0);
    }

    public function testIdleWorkersOnlyAttemptTheChainLockOncePerWindow(): void
    {
        for ($i = 0; $i < 20; $i++) {
            Watchdog::wake('redis');
        }

        $this->assertChainLockAttempts(1);
        Queue::assertNothingPushed();

        $this->travel(60)
            ->seconds();
        Watchdog::wake('redis');
        $this->assertChainLockAttempts(2);
    }

    public function testExpiredThrottleIsRenewedWithoutDeletingOrReinsertingIt(): void
    {
        $this->seedExpiredThrottle();
        $this->createPendingWorkflow();
        DB::connection('watchdog-cache')->flushQueryLog();

        Watchdog::wake('redis', 'high,default');
        Watchdog::wake('redis');

        Queue::assertPushed(Watchdog::class, 1);
        Queue::assertPushed(Watchdog::class, static function (Watchdog $watchdog): bool {
            return $watchdog->connection === 'redis' && $watchdog->queue === 'high'
                && Cache::get('workflow:watchdog:looping') === $watchdog->generation;
        });
        $this->assertChainLockAttempts(1);

        foreach (DB::connection('watchdog-cache')->getQueryLog() as $query) {
            if (in_array(self::PREFIX . 'workflow:watchdog:looping', $query['bindings'], true)) {
                $this->assertDoesNotMatchRegularExpression('/^(delete|insert)/i', $query['query']);
            }
        }
    }

    public function testExpiredMarkerCanBeReplacedAfterTheThrottleExpires(): void
    {
        $this->createPendingWorkflow();
        Watchdog::wake('redis');
        $firstGeneration = Cache::get('workflow:watchdog');

        $this->travel(361)
            ->seconds();
        Watchdog::wake('redis');

        Queue::assertPushed(Watchdog::class, 2);
        $this->assertNotSame($firstGeneration, Cache::get('workflow:watchdog'));
    }

    public function testFailedDispatchReleasesTheRenewedThrottleForImmediateRetry(): void
    {
        $this->seedExpiredThrottle();
        $this->createPendingWorkflow();
        $dispatcher = $this->app->make(Dispatcher::class);
        $failingDispatcher = $this->createStub(Dispatcher::class);
        $failingDispatcher->method('dispatch')
            ->willThrowException(new RuntimeException('dispatch failed'));
        $this->app->instance(Dispatcher::class, $failingDispatcher);

        try {
            Watchdog::wake('redis');
            $this->fail('Expected dispatch failure.');
        } catch (RuntimeException $exception) {
            $this->assertSame('dispatch failed', $exception->getMessage());
        }

        $this->assertFalse(Cache::has('workflow:watchdog'));
        $this->assertFalse(Cache::has('workflow:watchdog:looping'));
        $this->app->instance(Dispatcher::class, $dispatcher);

        Watchdog::wake('redis');
        Queue::assertPushed(Watchdog::class, 1);
    }

    public function testAnotherWorkerWinningTheInitialInsertDoesNotStartAChain(): void
    {
        $this->competeAfterThrottleRead(false);

        Watchdog::wake('redis');

        $this->assertSame('other-worker', Cache::get('workflow:watchdog:looping'));
        $this->assertChainLockAttempts(0);
        Queue::assertNothingPushed();
    }

    public function testAnotherWorkerRenewingTheExpiredThrottleDoesNotLoseItsLease(): void
    {
        $this->seedExpiredThrottle();
        $this->competeAfterThrottleRead(true);

        Watchdog::wake('redis');

        $this->assertSame('other-worker', Cache::get('workflow:watchdog:looping'));
        $this->assertChainLockAttempts(0);
        Queue::assertNothingPushed();
    }

    public function testDatabaseFailureIsNotSilenced(): void
    {
        Schema::connection('watchdog-cache')->drop('watchdog_cache');

        $this->expectException(QueryException::class);
        Watchdog::wake('redis');
    }

    private function competeAfterThrottleRead(bool $existing): void
    {
        $competed = false;
        DB::listen(static function (QueryExecuted $event) use (&$competed, $existing): void {
            if ($competed || ! str_starts_with($event->sql, 'select')
                || ! str_contains($event->sql, 'expiration')
                || ! in_array(self::PREFIX . 'workflow:watchdog:looping', $event->bindings, true)) {
                return;
            }

            $competed = true;
            $query = DB::connection('watchdog-cache')->table('watchdog_cache');
            $values = [
                'value' => serialize('other-worker'),
                'expiration' => now()
                    ->addMinute()
                    ->getTimestamp(),
            ];

            if ($existing) {
                $query->where('key', self::PREFIX . 'workflow:watchdog:looping')->update($values);
            } else {
                $query->insert([
                    'key' => self::PREFIX . 'workflow:watchdog:looping',
                    ...$values,
                ]);
            }
        });
    }

    private function assertChainLockAttempts(int $expected): void
    {
        $attempts = array_filter(DB::connection('watchdog-cache')->getQueryLog(), static function (array $query): bool {
            return str_starts_with(strtolower($query['query']), 'insert')
                && str_contains($query['query'], 'watchdog_locks');
        });
        $this->assertCount($expected, $attempts);
    }

    private function seedExpiredThrottle(): void
    {
        DB::connection('watchdog-cache')->table('watchdog_cache')->insert([
            'key' => self::PREFIX . 'workflow:watchdog:looping',
            'value' => serialize('expired-generation'),
            'expiration' => now()
                ->subSecond()
                ->getTimestamp(),
        ]);
    }

    private function createPendingWorkflow(): void
    {
        StoredWorkflow::create([
            'class' => TestSimpleWorkflow::class,
            'arguments' => Serializer::serialize([]),
            'status' => WorkflowPendingStatus::$name,
            'updated_at' => now()
                ->subSeconds(Watchdog::DEFAULT_TIMEOUT + 1),
        ]);
    }
}
