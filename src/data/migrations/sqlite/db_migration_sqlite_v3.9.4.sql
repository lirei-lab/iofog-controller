ALTER TABLE MicroserviceStatuses ADD COLUMN image_pull_status TEXT DEFAULT '';
ALTER TABLE Microservices ADD COLUMN generation INT DEFAULT 1;
ALTER TABLE Microservices ADD COLUMN rebuild_generation INT DEFAULT 0;
ALTER TABLE MicroserviceStatuses ADD COLUMN observed_generation INTEGER DEFAULT 0;
ALTER TABLE Fogs ADD COLUMN nats_config_status TEXT;
