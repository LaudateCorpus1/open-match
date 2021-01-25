// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statestore

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/cenkalti/backoff"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"open-match.dev/open-match/pkg/pb"
)

const (
	allTickets        = "allTickets"
	proposedTicketIDs = "proposed_ticket_ids"
)

// CreateTicket creates a new Ticket in the state storage. If the id already exists, it will be overwritten.
func (rb *redisBackend) CreateTicket(ctx context.Context, ticket *pb.Ticket) error {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "CreateTicket, id: %s, failed to connect to redis: %v", ticket.GetId(), err)
	}

	value, err := proto.Marshal(ticket)
	if err != nil {
		err = errors.Wrapf(err, "failed to marshal the ticket proto, id: %s", ticket.GetId())
		return status.Errorf(codes.Internal, "%v", err)
	}

	_, err = redisConn.Set(ctx, ticket.GetId(), value, 0).Result()
	if err != nil {
		err = errors.Wrapf(err, "failed to set the value for ticket, id: %s", ticket.GetId())
		return status.Errorf(codes.Internal, "%v", err)
	}

	return nil
}

// GetTicket gets the Ticket with the specified id from state storage. This method fails if the Ticket does not exist.
func (rb *redisBackend) GetTicket(ctx context.Context, id string) (*pb.Ticket, error) {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "GetTicket, id: %s, failed to connect to redis: %v", id, err)
	}

	value, err := redisConn.Get(ctx, id).Bytes()
	if err != nil {
		// Return NotFound if redigo did not find the ticket in storage.
		if err == redis.Nil {
			msg := fmt.Sprintf("Ticket id: %s not found", id)
			return nil, status.Error(codes.NotFound, msg)
		}

		err = errors.Wrapf(err, "failed to get the ticket from state storage, id: %s", id)
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	if value == nil {
		msg := fmt.Sprintf("Ticket id: %s not found", id)
		return nil, status.Error(codes.NotFound, msg)
	}

	ticket := &pb.Ticket{}
	err = proto.Unmarshal(value, ticket)
	if err != nil {
		err = errors.Wrapf(err, "failed to unmarshal the ticket proto, id: %s", id)
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	return ticket, nil
}

// DeleteTicket removes the Ticket with the specified id from state storage.
func (rb *redisBackend) DeleteTicket(ctx context.Context, id string) error {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "DeleteTicket, id: %s, failed to connect to redis: %v", id, err)
	}

	_, err = redisConn.Del(ctx, id).Result()
	if err != nil {
		err = errors.Wrapf(err, "failed to delete the ticket from state storage, id: %s", id)
		return status.Errorf(codes.Internal, "%v", err)
	}

	return nil
}

// IndexTicket indexes the Ticket id for the configured index fields.
func (rb *redisBackend) IndexTicket(ctx context.Context, ticket *pb.Ticket) error {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "IndexTicket, id: %s, failed to connect to redis: %v", ticket.GetId(), err)
	}

	_, err = redisConn.SAdd(ctx, allTickets, ticket.Id).Result()
	if err != nil {
		err = errors.Wrapf(err, "failed to add ticket to all tickets, id: %s", ticket.Id)
		return status.Errorf(codes.Internal, "%v", err)
	}

	return nil
}

// DeindexTicket removes the indexing for the specified Ticket. Only the indexes are removed but the Ticket continues to exist.
func (rb *redisBackend) DeindexTicket(ctx context.Context, id string) error {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "DeindexTicket, id: %s, failed to connect to redis: %v", id, err)
	}

	_, err = redisConn.SRem(ctx, allTickets, id).Result()
	if err != nil {
		err = errors.Wrapf(err, "failed to remove ticket from all tickets, id: %s", id)
		return status.Errorf(codes.Internal, "%v", err)
	}

	return nil
}

// GetIndexedIds returns the ids of all tickets currently indexed.
func (rb *redisBackend) GetIndexedIDSet(ctx context.Context) (map[string]struct{}, error) {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "GetIndexedIDSet, failed to connect to redis: %v", err)
	}

	ttl := rb.cfg.GetDuration("pendingReleaseTimeout")
	curTime := time.Now()
	endTimeInt := curTime.Add(time.Hour).UnixNano()
	startTimeInt := curTime.Add(-ttl).UnixNano()

	// Filter out tickets that are fetched but not assigned within ttl time (ms).
	idsInPendingReleases, err := redisConn.ZRangeByScore(ctx, proposedTicketIDs, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", startTimeInt),
		Max: fmt.Sprintf("%d", endTimeInt),
	}).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting pending release %v", err)
	}

	idsIndexed, err := redisConn.SMembers(ctx, allTickets).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting all indexed ticket ids %v", err)
	}

	r := make(map[string]struct{}, len(idsIndexed))
	for _, id := range idsIndexed {
		r[id] = struct{}{}
	}
	for _, id := range idsInPendingReleases {
		delete(r, id)
	}

	return r, nil
}

// GetTickets returns multiple tickets from storage.  Missing tickets are
// silently ignored.
func (rb *redisBackend) GetTickets(ctx context.Context, ids []string) ([]*pb.Ticket, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "GetTickets, failed to connect to redis: %v", err)
	}

	tickets, err := redisConn.MGet(ctx, ids...).Result()
	if err != nil {
		err = errors.Wrapf(err, "failed to lookup tickets %v", ids)
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	r := make([]*pb.Ticket, 0, len(ids))

	for i, b := range tickets {
		// Tickets may be deleted by the time we read it from redis.
		if b != nil {
			data, ok := b.(string)
			if !ok {
				redisLogger.WithField("id", ids[i]).Warn("returned value was not valid")
				continue
			}
			t := &pb.Ticket{}
			err = proto.Unmarshal([]byte(data), t)
			if err != nil {
				err = errors.Wrapf(err, "failed to unmarshal ticket from redis, key %s", ids[i])
				return nil, status.Errorf(codes.Internal, "%v", err)
			}
			r = append(r, t)
		}
	}

	return r, nil
}

// UpdateAssignments update using the request's specified tickets with assignments.
func (rb *redisBackend) UpdateAssignments(ctx context.Context, req *pb.AssignTicketsRequest) (*pb.AssignTicketsResponse, []*pb.Ticket, error) {
	resp := &pb.AssignTicketsResponse{}
	if len(req.Assignments) == 0 {
		return resp, []*pb.Ticket{}, nil
	}

	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return nil, nil, status.Errorf(codes.Unavailable, "UpdateAssignments, failed to connect to redis: %v", err)
	}

	idToA := make(map[string]*pb.Assignment)
	ids := make([]string, 0)
	for _, a := range req.Assignments {
		if a.Assignment == nil {
			return nil, nil, status.Error(codes.InvalidArgument, "AssignmentGroup.Assignment is required")
		}

		for _, id := range a.TicketIds {
			if _, ok := idToA[id]; ok {
				return nil, nil, status.Errorf(codes.InvalidArgument, "Ticket id %s is assigned multiple times in one assign tickets call", id)
			}

			idToA[id] = a.Assignment
			ids = append(ids, id)
		}
	}

	ticketSlice, err := redisConn.MGet(ctx, ids...).Result()
	if err != nil {
		return nil, nil, err
	}

	tickets := make([]*pb.Ticket, 0, len(ticketSlice))
	for i, ticket := range ticketSlice {
		// Tickets may be deleted by the time we read it from redis.
		if ticket == nil {
			resp.Failures = append(resp.Failures, &pb.AssignmentFailure{
				TicketId: ids[i],
				Cause:    pb.AssignmentFailure_TICKET_NOT_FOUND,
			})
		} else {
			data, ok := ticket.(string)
			if !ok {
				redisLogger.WithField("id", ids[i]).Warn("returned value was not valid")
				continue
			}
			t := &pb.Ticket{}
			err = proto.Unmarshal([]byte(data), t)
			if err != nil {
				err = errors.Wrapf(err, "failed to unmarshal ticket from redis %s", ids[i])
				return nil, nil, status.Errorf(codes.Internal, "%v", err)
			}
			tickets = append(tickets, t)
		}
	}
	assignmentTimeout := rb.cfg.GetDuration("assignedDeleteTimeout")
	tx := redisConn.TxPipeline()

	for _, ticket := range tickets {
		ticket.Assignment = idToA[ticket.Id]

		var ticketByte []byte
		ticketByte, err = proto.Marshal(ticket)
		if err != nil {
			return nil, nil, status.Errorf(codes.Internal, "failed to marshal ticket %s", ticket.GetId())
		}

		tx.Set(ctx, ticket.Id, ticketByte, assignmentTimeout)
	}

	wasSet, err := tx.Exec(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error executing assignment set")
	}

	if len(wasSet) != len(tickets) {
		return nil, nil, status.Errorf(codes.Internal, "sent %d tickets to redis, but received %d back", len(tickets), len(wasSet))
	}

	assignedTickets := make([]*pb.Ticket, 0, len(tickets))
	for i, ticket := range tickets {
		err = wasSet[i].Err()
		if err == redis.Nil {
			resp.Failures = append(resp.Failures, &pb.AssignmentFailure{
				TicketId: ticket.Id,
				Cause:    pb.AssignmentFailure_TICKET_NOT_FOUND,
			})
			continue
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "unexpected error from redis multi set")
		}

		v, ok := wasSet[i].(*redis.StatusCmd)
		if !ok {
			return nil, nil, status.Errorf(codes.Internal, "couldn't parse redis command response: %s", wasSet[i].String())
		}

		if v.Val() != "OK" {
			return nil, nil, status.Errorf(codes.Internal, "unexpected response from redis: %s", v.Val())
		}
		assignedTickets = append(assignedTickets, ticket)
	}

	return resp, assignedTickets, nil
}

// GetAssignments returns the assignment associated with the input ticket id
func (rb *redisBackend) GetAssignments(ctx context.Context, id string, callback func(*pb.Assignment) error) error {
	_, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "GetAssignments, id: %s, failed to connect to redis: %v", id, err)
	}

	backoffOperation := func() error {
		var ticket *pb.Ticket
		ticket, err = rb.GetTicket(ctx, id)
		if err != nil {
			return backoff.Permanent(err)
		}

		err = callback(ticket.GetAssignment())
		if err != nil {
			return backoff.Permanent(err)
		}

		return status.Error(codes.Unavailable, "listening on assignment updates, waiting for the next backoff")
	}

	err = backoff.Retry(backoffOperation, rb.newConstantBackoffStrategy())
	if err != nil {
		return err
	}
	return nil
}

// AddTicketsToPendingRelease appends new proposed tickets to the proposed sorted set with current timestamp
func (rb *redisBackend) AddTicketsToPendingRelease(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "AddTicketsToPendingRelease, failed to connect to redis: %v", err)
	}

	currentTime := time.Now().UnixNano()
	cmds := make([]*redis.Z, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, &redis.Z{
			Score:  float64(currentTime),
			Member: id,
		})
	}

	_, err = redisConn.ZAdd(ctx, proposedTicketIDs, cmds...).Result()
	if err != nil {
		err = errors.Wrap(err, "failed to append proposed tickets to pending release")
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

// DeleteTicketsFromPendingRelease deletes tickets from the proposed sorted set
func (rb *redisBackend) DeleteTicketsFromPendingRelease(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "DeleteTicketsFromPendingRelease, failed to connect to redis: %v", err)
	}

	idsI := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		idsI = append(idsI, id)
	}

	_, err = redisConn.ZRem(ctx, proposedTicketIDs, idsI...).Result()
	if err != nil {
		err = errors.Wrap(err, "failed to delete proposed tickets from pending release")
		return status.Error(codes.Internal, err.Error())
	}

	return nil
}

func (rb *redisBackend) ReleaseAllTickets(ctx context.Context) error {
	redisConn, err := rb.GetConnection(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "ReleaseAllTickets, failed to connect to redis: %v", err)
	}

	_, err = redisConn.Del(ctx, proposedTicketIDs).Result()
	return err
}

func (rb *redisBackend) newConstantBackoffStrategy() backoff.BackOff {
	backoffStrat := backoff.NewConstantBackOff(rb.cfg.GetDuration("backoff.initialInterval"))
	return backoff.BackOff(backoffStrat)
}

// TODO: add cache the backoff object
// nolint: unused
func (rb *redisBackend) newExponentialBackoffStrategy() backoff.BackOff {
	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.InitialInterval = rb.cfg.GetDuration("backoff.initialInterval")
	backoffStrat.RandomizationFactor = rb.cfg.GetFloat64("backoff.randFactor")
	backoffStrat.Multiplier = rb.cfg.GetFloat64("backoff.multiplier")
	backoffStrat.MaxInterval = rb.cfg.GetDuration("backoff.maxInterval")
	backoffStrat.MaxElapsedTime = rb.cfg.GetDuration("backoff.maxElapsedTime")
	return backoff.BackOff(backoffStrat)
}
