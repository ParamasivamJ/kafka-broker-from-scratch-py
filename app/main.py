try:
    from .kafka_server import main
except ImportError:
    from kafka_server import main


if __name__ == "__main__":
    main()
