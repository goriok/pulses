![CI](https://github.com/goriok/pulses/actions/workflows/build.yml/badge.svg)

# Pulses

## Getting Started with Devbox

To simplify the developer setup process, we use [Devbox](https://www.jetpack.io/devbox/). Follow these steps to get started:

1. Install Devbox by following the [installation guide](https://www.jetpack.io/devbox/docs/install/).
2. Clone the repository:
   ```bash
   git clone https://github.com/your-username/pulses.git
   cd pulses
   ```
3. Start the Devbox environment:
   ```bash
   devbox shell
   ```
4. Install project dependencies:
   ```bash
   devbox install
   ```

You're now ready to start developing!

## Documentation

We use [gomarkdoc](https://github.com/princjef/gomarkdoc) to generate and embed Go documentation directly into this README. The documentation includes both exported and unexported symbols for comprehensive coverage.

### Generating Documentation

To generate and embed documentation into this README, run:

```bash
just docs
```

---

<!-- gomarkdoc:embed:start -->

<!-- Code generated by gomarkdoc. DO NOT EDIT -->

# pulses

```go
import "goriok/pulses"
```

## Index

- [Constants](#constants)
- [func main\(\)](#main)

## Constants

<a name="brokerPort"></a>

```go
const (
    brokerPort    = 9000
    pulsesSubject = "cloud.sku.pulses"
)
```

<a name="main"></a>

## func [main](https://github.com/goriok/pulses/blob/main/main.go#L17)

```go
func main()
```

Generated by [gomarkdoc](https://github.com/princjef/gomarkdoc)


<!-- Code generated by gomarkdoc. DO NOT EDIT -->

# pulses

```go
import "goriok/pulses"
```

## Index

- [Constants](<#constants>)
- [func main\(\)](<#main>)


## Constants

<a name="brokerPort"></a>

```go
const (
    brokerPort    = 9000
    pulsesSubject = "cloud.sku.pulses"
)
```

<a name="main"></a>
## func [main](<https://github.com/goriok/pulses/blob/main/main.go#L17>)

```go
func main()
```



Generated by [gomarkdoc](<https://github.com/princjef/gomarkdoc>)
