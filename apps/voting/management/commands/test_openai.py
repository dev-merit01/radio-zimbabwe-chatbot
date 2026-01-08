"""Test the OpenAI API key by making a simple GPT-4o-mini request.

Usage:
    python manage.py test_openai
"""
from django.core.management.base import BaseCommand

from apps.voting.llm_matcher import call_openai_api, get_openai_api_key


class Command(BaseCommand):
    help = "Test OpenAI API connectivity and key validity"

    def handle(self, *args, **options):
        self.stdout.write("🔍 Testing OpenAI API configuration...")

        # Check that a key is configured
        try:
            api_key = get_openai_api_key()
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"❌ OpenAI API key not configured: {exc}"))
            return

        if not api_key:
            self.stdout.write(self.style.ERROR("❌ OPENAI_API_KEY is empty"))
            return

        # Show masked key for verification
        masked = api_key[:8] + "..." + api_key[-4:]
        self.stdout.write(self.style.SUCCESS(f"✅ OPENAI_API_KEY is set: {masked}"))
        self.stdout.write("Making a test request to GPT-4o-mini...")

        test_prompt = (
            "You are a health-check endpoint. Reply with exactly: OK\n"
            "No explanation, no punctuation, just OK."
        )

        try:
            response_text = call_openai_api(test_prompt)
        except Exception as exc:
            self.stdout.write(self.style.ERROR("❌ OpenAI API request failed."))
            self.stdout.write(self.style.ERROR(str(exc)))
            # Try to show more detail for HTTP errors
            try:
                import requests
                if isinstance(exc, requests.HTTPError) and exc.response is not None:
                    self.stdout.write(self.style.ERROR(
                        f"HTTP {exc.response.status_code}: {exc.response.text[:500]}"
                    ))
            except Exception:
                pass
            return

        response_text = (response_text or "").strip()
        self.stdout.write(self.style.SUCCESS("✅ OpenAI API call succeeded!"))
        
        if response_text:
            preview = response_text[:200].replace("\n", " ")
            self.stdout.write(f"Response: {preview}")
        else:
            self.stdout.write("⚠️ Call succeeded but response was empty.")

        self.stdout.write(self.style.SUCCESS("\n🎉 OpenAI GPT-4o-mini is ready for LLM matching!"))
