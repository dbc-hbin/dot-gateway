package bridge

import (
	"context"
	"errors"

	"github.com/bwmarrin/discordgo"
)

// RecoverThreadMessage retrieves one explicitly identified real Discord message.
// It never synthesizes content, enumerates guild history, joins or unarchives a
// thread, or sends a reply. Normal durable validation still runs after staging.
func (r *RESTClient) RecoverThreadMessage(ctx context.Context, store *Store, threadID, messageID string) (string, error) {
	if !Snowflake(threadID) || !Snowflake(messageID) || threadID == r.settings.Policy.GuildChannelID || r.settings.Policy.GuildID == "" {
		return "", errors.New("invalid_thread_recovery_route")
	}
	if _, err := r.Identity(ctx); err != nil {
		return "", err
	}
	thread, err := r.Channel(ctx, threadID)
	if err != nil {
		return "", err
	}
	target := Envelope{ConversationID: threadID, GuildID: r.settings.Policy.GuildID, RouteKind: "guild_thread", ParentChannelID: r.settings.Policy.GuildChannelID, ThreadType: thread.Type}
	if err = r.validateThreadRoute(ctx, thread, target, 0); err != nil {
		return "", err
	}
	var message discordgo.Message
	if err = r.get(ctx, "/channels/"+threadID+"/messages/"+messageID, &message); err != nil {
		return "", err
	}
	if message.ID != messageID || message.ChannelID != threadID || (message.GuildID != "" && message.GuildID != thread.GuildID) {
		return "", errors.New("preflight_message_mismatch")
	}
	// REST message responses may omit guild_id. Its route comes from the exact
	// authenticated channel GET, not user content or a caller-supplied envelope.
	message.GuildID = thread.GuildID
	return receiveMessage(ctx, r, store, r.settings, &message)
}
