CREATE TABLE IF NOT EXISTS covid_country_daily (
  country        VARCHAR(100) NOT NULL,
  report_date    DATE         NOT NULL,
  confirmed      BIGINT,
  recovered      BIGINT,
  deaths         BIGINT,
  active         BIGINT,
  mortality_rate DOUBLE PRECISION,
  recovery_rate  DOUBLE PRECISION,
  new_cases      BIGINT,
  new_deaths     BIGINT,
  processed_at   TIMESTAMP DEFAULT now(),
  PRIMARY KEY (country, report_date)
);

CREATE TABLE IF NOT EXISTS covid_stage (
  country        VARCHAR(100),
  report_date    DATE,
  confirmed      BIGINT,
  recovered      BIGINT,
  deaths         BIGINT,
  active         BIGINT,
  mortality_rate DOUBLE PRECISION,
  recovery_rate  DOUBLE PRECISION,
  new_cases      BIGINT,
  new_deaths     BIGINT,
  processed_at   TIMESTAMP DEFAULT now()
);

CREATE OR REPLACE FUNCTION upsert_covid() RETURNS trigger AS $$
BEGIN
  INSERT INTO covid_country_daily
    (country, report_date, confirmed, recovered, deaths, active,
     mortality_rate, recovery_rate, new_cases, new_deaths, processed_at)
  VALUES
    (NEW.country, NEW.report_date, NEW.confirmed, NEW.recovered, NEW.deaths, NEW.active,
     NEW.mortality_rate, NEW.recovery_rate, NEW.new_cases, NEW.new_deaths, NEW.processed_at)
  ON CONFLICT (country, report_date) DO UPDATE SET
    confirmed = EXCLUDED.confirmed, recovered = EXCLUDED.recovered,
    deaths = EXCLUDED.deaths, active = EXCLUDED.active,
    mortality_rate = EXCLUDED.mortality_rate, recovery_rate = EXCLUDED.recovery_rate,
    new_cases = EXCLUDED.new_cases, new_deaths = EXCLUDED.new_deaths,
    processed_at = EXCLUDED.processed_at;
  RETURN NEW;
END $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_upsert ON covid_stage;
CREATE TRIGGER trg_upsert AFTER INSERT ON covid_stage
FOR EACH ROW EXECUTE FUNCTION upsert_covid();
