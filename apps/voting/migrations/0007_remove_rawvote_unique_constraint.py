# Generated migration to remove unique_together constraint on RawVote
# This allows privileged user (263786326862) to vote for the same song multiple times per day

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('voting', '0006_add_llm_decision_log'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='rawvote',
            unique_together=set(),
        ),
        migrations.AddIndex(
            model_name='rawvote',
            index=models.Index(fields=['user', 'vote_date'], name='voting_rawv_user_id_vote_date_idx'),
        ),
    ]
