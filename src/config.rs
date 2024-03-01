use std::{collections::HashSet, fmt::Display, time::Duration};

use anyhow::anyhow;

use crate::args::Cli;

const DEFAULT_STEPS: &str = "dtpfvg";

#[derive(Debug, Clone)]
pub struct Config {
    pub init_steps: InitSteps,
    pub mode: BenchMod,
    pub fillfactor: u64,
    pub jobs: u64,
    pub time: Duration,
    pub vebose_errors: bool,
    // transactions per connection
    pub transactions: u64,
    // transactions summary for all connections
    pub transactions_total: u64,
    pub instances: Vec<ConnectionConfig>,
    pub test_config: TestConfig,
    pub max_retries: u64,
    pub bucket_count: u32,
    pub keep_history: bool,
}

impl Config {
    pub fn contains_step(&self, step: &InitStep) -> bool {
        self.init_steps.0.contains(step)
    }
}

#[derive(Debug, Clone)]
pub struct InitSteps(HashSet<InitStep>);

impl<'a> TryFrom<&'a str> for InitSteps {
    type Error = anyhow::Error;

    fn try_from(steps: &'a str) -> Result<Self, Self::Error> {
        Ok(Self(
            steps
                .chars()
                .into_iter()
                .map(|step| InitStep::try_from(step))
                .collect::<Result<HashSet<InitStep>, anyhow::Error>>()?,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub addr: &'static str,
    pub timeout: Option<Duration>,
    pub user: &'static str,
    pub password: Option<&'static str>,
    pub connections: usize,
}

#[derive(Debug, Clone)]
pub struct TestConfig {
    pub ttbench_branches: u64,
    pub ttbench_tellers: u64,
    pub ttbench_accounts: u64,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            ttbench_branches: 1,
            ttbench_tellers: 10,
            ttbench_accounts: 100_000,
        }
    }
}

impl TryFrom<u64> for TestConfig {
    type Error = anyhow::Error;

    fn try_from(scale: u64) -> Result<Self, Self::Error> {
        if scale == 0 {
            return Err(anyhow!("scale can't be 0"));
        }

        println!("scaling factor: {}", scale);

        Ok(TestConfig {
            ttbench_branches: 1 * scale,
            ttbench_tellers: 10 * scale,
            ttbench_accounts: 100_000 * scale,
        })
    }
}

#[derive(Debug, Clone)]
pub enum BenchMod {
    Iterations,
    Time,
}

impl Display for BenchMod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchMod::Iterations => write!(f, "iterations"),
            BenchMod::Time => write!(f, "time"),
        }
    }
}

impl Config {
    pub fn new(cli: &Cli) -> Result<Self, anyhow::Error> {
        let connections = 3;
        let iterations_count = 1000;
        let mode = if cli.transactions.is_some() {
            BenchMod::Iterations
        } else {
            BenchMod::Time
        };
        println!("transaction type: {}", "<builtin: TCP-B>"); // TODO: отображать режим транзакций
        println!("number of jobs: {}", cli.jobs);
        println!("number of connections: {}", cli.connections);
        println!(
            "number transactions per connection: {}",
            cli.transactions
                .map(|num| num.to_string())
                .unwrap_or("∞".to_string())
        );
        println!("benchmark mode: {mode}");
        Ok(Config {
            mode,
            test_config: TestConfig::try_from(cli.scale)?,
            init_steps: InitSteps::try_from(
                cli.init_steps
                    .as_ref()
                    .map(|steps| steps.as_str())
                    .unwrap_or(DEFAULT_STEPS),
            )?,
            fillfactor: 100, // TODO
            jobs: cli.jobs,
            transactions: cli.transactions.unwrap_or(0),
            time: Duration::from_secs(cli.time),
            vebose_errors: false, // TODO
            // TODO
            instances: {
                vec![ConnectionConfig {
                    addr: "localhost:3031",
                    timeout: Some(Duration::from_millis(500)),
                    user: "admin",
                    password: Some("admin"),
                    connections,
                }]
            },
            transactions_total: { iterations_count },
            max_retries: u64::MAX, // TODO
            bucket_count: 30000,   // TODO
            keep_history: cli.keep_history,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum InitStep {
    Drop,
    Create,
    Primary,
    Foreign,
    GenerateData,
    Vacuum,
}

impl TryFrom<char> for InitStep {
    type Error = anyhow::Error;

    fn try_from(ch: char) -> Result<Self, Self::Error> {
        match ch {
            'd' => Ok(Self::Drop),
            't' => Ok(Self::Create),
            'p' => Ok(Self::Primary),
            'f' => Ok(Self::Foreign),
            'v' => Ok(Self::Vacuum),
            'G' | 'g' => Ok(Self::GenerateData),
            symbol => Err(anyhow!("unknown init step '{symbol}'")),
        }
    }
}
