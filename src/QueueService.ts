import { Rabbit } from '@danielemeryau/simple-rabbitmq';

import {
  ILogger,
  IProcessManagerService,
  IServiceHealth,
  IServiceStatus,
} from '@teamest/process-manager';

export interface IQueueServiceOptions {
  host: string;
  port: number;
  user: string;
  password: string;
}

export default class QueueService implements IProcessManagerService {
  logger: ILogger;
  options: IQueueServiceOptions;
  rabbit?: Rabbit;
  state: 'stopped' | 'starting' | 'running' | 'stopping';

  constructor(logger: ILogger, options: IQueueServiceOptions) {
    this.logger = logger;
    this.options = options;
    this.state = 'stopped';
  }

  getInstance(): Rabbit {
    if (!this.rabbit) {
      throw new Error('Queue not ready');
    }
    return this.rabbit;
  }

  getName(): string {
    return 'queue';
  }

  async getHealth(): Promise<IServiceHealth> {
    if (this.rabbit) {
      try {
        await this.rabbit.raw('select 1+1 as result');
        return {
          healthy: 'healthy',
        };
      } catch (err) {
        return {
          healthy: 'unhealthy',
          message: `Failed to connect to RabbitMQ: ${err}`,
        };
      }
    }
    return {
      healthy: 'unhealthy',
      message: 'RabbitMQ instance not initialised',
    };
  }

  getStatus(): IServiceStatus {
    return {
      state: this.state,
    };
  }

  async start(): Promise<void> {
    this.state = 'starting';
    this.logger.info(
      `Connecting to MySql ${this.options.user}@${this.options.host}`,
    );
    this.knex = Knex({
      client: 'mysql2',
      connection: this.options,
      migrations: {
        tableName: 'migrations',
      },
    });
    try {
      await this.knex.raw('select 1+1 as result');
    } finally {
      this.state = 'running';
    }
  }

  stop(): Promise<void> {
    this.logger.info('Attempting to close mysql connections')
    this.state = 'stopping';
    if (this.knex) {
      return new Promise((res, rej) => {
        try {
          this.knex?.destroy(() => {
            this.logger.info('mysql connections closed successfully');
            this.state = 'stopped';
            res();
          });
        } catch (err) {
          rej(err);
        }
      });
    } else {
      this.state = 'stopped';
      this.logger.info('No connections to close, stopped');
      return Promise.resolve();
    }
  }
}
