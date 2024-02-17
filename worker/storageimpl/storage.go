package storageimpl

import (
	"bufio"
	"io"
	"log"
	"main/utils"
	"main/worker/domain"
	"os"
	"path/filepath"
)

type NotificationStorageFactoryImpl struct {
	workerId string
}

func NewNotificationStorageFactoryImpl(workerId string) *NotificationStorageFactoryImpl {
	return &NotificationStorageFactoryImpl{workerId: workerId}
}

func (n *NotificationStorageFactoryImpl) BuildNotificationStorage(identifier string) domain.NotificationStorage {
	return NewLoggedNotificationStorage(n.workerId, identifier)
}

type LoggedNotificationStorage struct {
	notifications *utils.MapSet[domain.Notification]
	logFile       *os.File

	separator              string
	isSynchronizedWithFile bool
}

func NewLoggedNotificationStorage(workerId string, identifier string) *LoggedNotificationStorage {
	f, err := os.OpenFile(filepath.Join(os.Getenv("WORKER_DATA"), "worker-"+workerId+"-log-"+identifier+".txt"), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	return &LoggedNotificationStorage{
		notifications: utils.NewMapSet[domain.Notification](),
		logFile:       f,
		separator:     "\n",
	}

}

func (l *LoggedNotificationStorage) AddNotification(notification domain.Notification) error {
	if !l.notifications.Contains(notification) {
		_, err := l.logFile.WriteString("+" + notification.PhyPartitionId.String() + l.separator)
		if err != nil {
			return err
		}
	}
	l.notifications.Add(notification)
	return nil
}

func (l *LoggedNotificationStorage) RemoveNotification(notification domain.Notification) error {
	if l.notifications.Contains(notification) {
		_, err := l.logFile.WriteString("-" + notification.PhyPartitionId.String() + l.separator)
		if err != nil {
			return err
		}
		l.notifications.Remove(notification)
	}
	return nil
}

func (l *LoggedNotificationStorage) RemoveAllNotifications(notifications ...domain.Notification) error {
	for _, notification := range notifications {
		err := l.RemoveNotification(notification)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *LoggedNotificationStorage) GetAllNotifications() []domain.Notification {
	var notifications []domain.Notification
	l.notifications.ForEach(func(notification domain.Notification) bool {
		notifications = append(notifications, notification)
		return false
	})

	return notifications
}

func (l *LoggedNotificationStorage) Close() error {
	return l.logFile.Close()
}

func (l *LoggedNotificationStorage) sync() error {
	l.notifications.Clear()
	rd := bufio.NewReader(l.logFile)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		phyPartitionId, _ := domain.StrToPhyPartitionId(line[1:])
		if line[0] == '+' {
			l.notifications.Add(domain.Notification{PhyPartitionId: phyPartitionId})
		} else if line[0] == '-' {
			l.notifications.Remove(domain.Notification{PhyPartitionId: phyPartitionId})
		} else {
			log.Fatalf("Found a corrupted log while synching")
		}

	}

	err := l.logFile.Truncate(0)
	if err != nil {
		return err
	}

	_, err = l.logFile.Seek(0, 0)
	if err != nil {
		return err
	}

	for _, notification := range l.GetAllNotifications() {
		err = l.AddNotification(notification)
		if err != nil {
			log.Fatalf("Failed to sync the log: %v", err)
		}
	}

	return nil
}
