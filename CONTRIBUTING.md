# Contributing to FleetPulse

Thank you for your interest in contributing to FleetPulse! This document provides guidelines for contributing to this data engineering portfolio project.

## Development Setup

### Prerequisites
- Python 3.11 or higher
- Docker Desktop (for full pipeline mode)
- Git

### Local Development Environment

1. **Clone the repository**
   ```bash
   git clone https://github.com/JuniorDieka/fleetpulse.git
   cd fleetpulse
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   make install
   # or manually:
   pip install -r requirements.txt
   pre-commit install
   ```

4. **Run tests**
   ```bash
   make test
   ```

## Code Standards

### Python Code Style
- **Formatter:** Black (line length: 100)
- **Linter:** Ruff
- **Type Checker:** MyPy
- All code must pass `make lint` before committing

### Pre-commit Hooks
Pre-commit hooks are configured to automatically:
- Format code with Black
- Lint with Ruff
- Type-check with MyPy
- Check for trailing whitespace, large files, merge conflicts

Install hooks with: `pre-commit install`

### Code Quality Requirements
- All functions must have type hints
- All modules must have docstrings
- Statistical functions must include formula documentation
- Test coverage should be maintained above 80%

## Testing

### Running Tests
```bash
# Run all tests
make test

# Run specific test file
pytest tests/test_reliability.py -v

# Run with coverage report
pytest --cov=fleetpulse --cov-report=html
```

### Test Requirements
- Unit tests for all statistical calculations (MTBF, MTTR, Weibull, Z-score)
- Use deterministic random seeds for reproducibility
- Test edge cases (zero failures, insufficient data, etc.)
- dbt tests must pass: `cd dbt_project && dbt test`

## Project Structure

```
fleetpulse/
├── fleetpulse/          # Main Python package
│   ├── simulator/       # IoT telemetry generation
│   ├── ingestion/       # Kafka consumer & Parquet writer
│   ├── analytics/       # Statistical analysis modules
│   └── alerter/         # Alert microservice
├── dags/                # Airflow DAG definitions
├── dbt_project/         # dbt models and tests
├── app/                 # Streamlit dashboard
└── tests/               # pytest test suite
```

## Making Changes

### Workflow
1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Run tests: `make test`
4. Run linters: `make lint`
5. Format code: `make format`
6. Commit with descriptive messages (see Commit Guidelines)
7. Push and create a Pull Request

### Commit Guidelines
Follow conventional commits format:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Adding or updating tests
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `chore:` Maintenance tasks

Example: `feat: add Weibull shape parameter interpretation to dashboard`

## Adding New Features

### New Statistical Metrics
1. Add calculation function to `fleetpulse/analytics/`
2. Add unit tests with known fixtures
3. Document the formula and interpretation
4. Update the Airflow DAG to call the new metric
5. Add visualization to Streamlit dashboard if applicable

### New dbt Models
1. Create SQL model in appropriate layer (staging/dimensions/marts)
2. Add schema.yml with column descriptions and tests
3. Document business logic in model header
4. Run `dbt run` and `dbt test` to validate
5. Update README with new model documentation

### New Dashboard Components
1. Add component to `app/components/`
2. Ensure it reads from DuckDB efficiently
3. Add appropriate caching with `@st.cache_data`
4. Test on both Windows and Linux
5. Update dashboard screenshots in README

## Configuration Changes

All configuration should be in `config.yaml`. Do not hardcode:
- Sensor ranges
- Thresholds
- File paths
- Connection strings
- Statistical parameters

## Documentation

### Code Documentation
- All public functions must have docstrings (Google style)
- Include parameter types, return types, and examples
- Document statistical formulas with LaTeX or plain text

### README Updates
Update README.md when:
- Adding new features
- Changing setup instructions
- Modifying architecture
- Adding dependencies

## Pull Request Process

1. **Description:** Clearly describe what the PR does and why
2. **Tests:** Ensure all tests pass
3. **Documentation:** Update relevant documentation
4. **Screenshots:** Include dashboard screenshots if UI changed
5. **Breaking Changes:** Clearly mark any breaking changes

### PR Checklist
- [ ] Tests pass (`make test`)
- [ ] Linters pass (`make lint`)
- [ ] Code is formatted (`make format`)
- [ ] Documentation updated
- [ ] Commit messages follow conventions
- [ ] No sensitive data or credentials committed

## Questions or Issues?

- Open an issue for bugs or feature requests
- Tag issues appropriately (bug, enhancement, documentation, etc.)
- Provide reproducible examples for bugs

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
