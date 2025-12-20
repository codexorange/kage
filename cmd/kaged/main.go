package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codexorange/kage/internal/protocol"
)

func main() {
	// 1. Inicializar Logger Estructurado
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 2. Configuración (Por ahora hardcoded, luego irá a internal/config)
	addr := "0.0.0.0:9092"

	// 3. Crear el listener TCP
	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		logger.Error("No se pudo abrir el puerto", "error", err, "addr", addr)
		os.Exit(1)
	}

	logger.Info("Kage Broker iniciado", "address", addr, "pid", os.Getpid())

	// 4. Canal para manejar el apagado (Graceful Shutdown)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Canal para errores del servidor
	serverError := make(chan error, 1)

	// 5. Iniciar el loop de aceptación en una goroutine
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				serverError <- err
				return
			}
			go handleConnection(conn, logger)
		}
	}()

	// 6. Esperar a una señal de stop o un error fatal
	select {
	case sig := <-stop:
		logger.Info("Apagando Kage...", "signal", sig.String())
	case err := <-serverError:
		logger.Error("Error crítico en el servidor", "error", err)
	}

	// 7. Cierre limpio
	// Damos un margen de tiempo para cerrar conexiones activas
	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener.Close()
	logger.Info("Kage se detuvo correctamente")
}

func handleConnection(conn net.Conn, logger *slog.Logger) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()
	logger.Debug("Nueva conexión establecida", "client", remoteAddr)

	decoder := protocol.NewDecoder(conn)

	// Intentamos leer la cabecera de la petición
	header, err := decoder.ParseRequestHeader()
	if err != nil {
		if err != os.ErrClosed {
			logger.Error("Error al parsear la cabecera", "client", remoteAddr, "error", err)
		}
		return
	}

	logger.Info("Petición recibida",
		"client", remoteAddr,
		"api_key", header.ApiKey,
		"version", header.ApiVersion,
		"correlation_id", header.CorrelationID,
	)

	// TODO: Aquí llamaremos al Handler para generar la respuesta (Encoder)
}
